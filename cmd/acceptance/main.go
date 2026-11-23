// 验收程序:在仿真链路上同时跑正常流量和压测流量,验证流量隔离中间层。
//   阶段 A:全服务接入中间层 —— 零污染、透传成功率、影子隔离、额外延迟
//   阶段 B:payment 人为不接入中间层 —— 漏标检测多快发现并熔断
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"stresstest/internal/infra"
	"stresstest/internal/middleware"
	"stresstest/internal/services"
)

var failed int

func check(name string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		failed++
	}
	fmt.Printf("  [%s] %s %s\n", status, name, detail)
}

func sendOrder(userID int64, stress bool) (time.Duration, int, error) {
	body, _ := json.Marshal(map[string]any{"user_id": userID, "sku_id": "sku-1", "qty": 1})
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:18001/api/order", bytes.NewReader(body))
	if stress {
		req.Header.Set(middleware.StressHeader, "1")
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return time.Since(start), resp.StatusCode, nil
}

func main() {
	fmt.Println("================ 阶段 A:全服务接入中间层,正常+压测混合流量 ================")
	env := services.Start("")

	const normalN, stressN = 40, 40
	var wg sync.WaitGroup
	var normSum, stressSum, normErr, stressErr atomic.Int64
	start := time.Now()
	for i := 0; i < normalN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, code, err := sendOrder(int64(1000+i%10), false)
			if err != nil || code != 200 {
				normErr.Add(1)
			}
			normSum.Add(int64(d))
		}(i)
	}
	for i := 0; i < stressN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, code, err := sendOrder(int64(9000000+i), true)
			if err != nil || code != 200 {
				stressErr.Add(1)
			}
			stressSum.Add(int64(d))
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	env.MQ.Drain(5 * time.Second)
	env.Executor.Wait()
	time.Sleep(300 * time.Millisecond)

	fmt.Printf("混合流量:正常 %d 笔(失败 %d,平均 %v),压测 %d 笔(失败 %d,平均 %v),总耗时 %v\n\n",
		normalN, normErr.Load(), time.Duration(normSum.Load()/normalN),
		stressN, stressErr.Load(), time.Duration(stressSum.Load()/stressN), elapsed.Round(time.Millisecond))

	fmt.Println("--- 验收 1:真实环境零污染 ---")
	p := env.ScanPollution()
	check("真实库表无压测数据", p.RealTableRows == 0, fmt.Sprintf("(发现 %d 行)", p.RealTableRows))
	check("真实缓存无压测数据", p.RealCacheKeys == 0, fmt.Sprintf("(发现 %d 键)", p.RealCacheKeys))
	check("真实主题无压测消息", p.RealTopicMsgs == 0, fmt.Sprintf("(发现 %d 条)", p.RealTopicMsgs))
	check("第三方无压测调用", p.RealThirdCalls == 0, fmt.Sprintf("(发现 %d 次)", p.RealThirdCalls))
	for _, d := range p.Details {
		fmt.Println("    污染明细:", d)
	}

	fmt.Println("--- 验收 2:各调用方式标记透传成功率 ---")
	snap := env.Snapshot()
	syncOK := true
	names := make([]string, 0, len(snap.Metrics.Entries))
	for n := range snap.Metrics.Entries {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		st := snap.Metrics.Entries[n]
		rate := float64(0)
		if st.StressIn > 0 {
			rate = 100 * float64(st.Marked) / float64(st.StressIn)
		}
		if st.Lost > 0 {
			syncOK = false
		}
		fmt.Printf("    同步  %-14s 压测流入 %3d 带标 %3d 丢标 %3d 透传率 %.1f%%\n", n, st.StressIn, st.Marked, st.Lost, rate)
	}
	check("同步调用透传率 100%", syncOK, "")

	asyncOK := true
	svcNames := make([]string, 0, len(snap.Metrics.Propagation))
	for n := range snap.Metrics.Propagation {
		svcNames = append(svcNames, n)
	}
	sort.Strings(svcNames)
	for _, n := range svcNames {
		for _, ct := range []string{"mq", "threadpool"} {
			ps, ok := snap.Metrics.Propagation[n][ct]
			if !ok {
				continue
			}
			rate := 100 * float64(ps.OK) / float64(ps.Total)
			if ps.OK != ps.Total {
				asyncOK = false
			}
			fmt.Printf("    %-10s %-14s 透传 %d/%d 成功率 %.1f%%\n", ct, n, ps.OK, ps.Total, rate)
		}
	}
	check("MQ/线程池透传率 100%", asyncOK, "")

	fmt.Println("--- 验收 3:影子隔离与挡板 ---")
	check("影子表有压测写入", len(snap.Metrics.ShadowWrites) > 0, fmt.Sprintf("%v", snap.Metrics.ShadowWrites))
	check("压测缓存键带前缀隔离", snap.CacheStressKeys > 0, fmt.Sprintf("(压测键 %d 个)", snap.CacheStressKeys))
	check("影子主题有压测消息", snap.Topics["order_created"+infra.ShadowTopicSuffix] > 0,
		fmt.Sprintf("(order_created__stress %d 条)", snap.Topics["order_created"+infra.ShadowTopicSuffix]))
	check("第三方挡板拦截压测调用", len(snap.Metrics.MockCalls) > 0 && snap.ThirdStressCalls == 0,
		fmt.Sprintf("(mock %v,真实第三方压测调用 %d)", snap.Metrics.MockCalls, snap.ThirdStressCalls))

	stressCtx := middleware.WithStress(context.Background(), true)
	err := env.DBP.Insert(stressCtx, "no_such_table", infra.Row{"user_id": int64(9000001)})
	check("影子表不存在时拒绝写入", errors.Is(err, infra.ErrShadowNotFound), fmt.Sprintf("err=%v", err))
	check("拒绝后未回退写真实表", !env.DB.HasTable("no_such_table"), "")

	fmt.Println("--- 验收 4:中间层额外延迟 ---")
	latNames := make([]string, 0, len(snap.Metrics.Latency))
	for n := range snap.Metrics.Latency {
		latNames = append(latNames, n)
	}
	sort.Strings(latNames)
	for _, n := range latNames {
		l := snap.Metrics.Latency[n]
		fmt.Printf("    %-18s 调用 %5d 次  平均 %7.1fµs  P99 %7.1fµs  最大 %8.1fµs\n",
			n, l.Count, l.AvgMicros, l.P99Micros, l.MaxMicros)
	}
	fmt.Printf("    端到端:正常单均 %v,压测单均 %v(差值含挡板 mock 比真实第三方更快等因素)\n",
		time.Duration(normSum.Load()/normalN), time.Duration(stressSum.Load()/stressN))

	env.Stop()

	fmt.Println("\n================ 阶段 B:payment 人为不接入中间层(漏标演练) ================")
	middleware.M.Reset()
	env2 := services.Start("payment")

	t0 := time.Now()
	var detectAt time.Time
	sendOrder(9000001, true) // 第一笔压测流量即触发漏标
	for time.Since(t0) < 15*time.Second {
		if middleware.M.BreakerOpen() {
			detectAt = time.Now()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	check("漏标被发现", !detectAt.IsZero(), fmt.Sprintf("(从压测流量进入到熔断耗时 %v)", detectAt.Sub(t0).Round(time.Millisecond)))
	check("压测已熔断", middleware.M.BreakerOpen(), "")
	_, code, _ := sendOrder(9000002, true)
	check("熔断后压测流量在入口被拒绝", code == http.StatusServiceUnavailable, fmt.Sprintf("(HTTP %d)", code))
	leaks := middleware.M.Snapshot().Leaks
	check("产生漏标告警", len(leaks) > 0, fmt.Sprintf("(%d 条)", len(leaks)))
	for _, l := range leaks {
		fmt.Printf("    告警: [%s] %s: %s\n", l.Time.Format("15:04:05.000"), l.Component, l.Detail)
	}
	env2.Stop()

	fmt.Println()
	if failed > 0 {
		fmt.Printf("验收失败:%d 项未通过\n", failed)
		os.Exit(1)
	}
	fmt.Println("全部验收通过 ✅")
}
