// 启动仿真电商链路 + 监控大盘。
//   go run ./cmd/server            全服务接入中间层
//   go run ./cmd/server -rogue=payment   payment 不接入中间层(演示漏标检测与熔断)
package main

import (
	"flag"
	"fmt"
	"net/http"
	"sort"

	"stresstest/internal/monitor"
	"stresstest/internal/services"
)

func main() {
	rogue := flag.String("rogue", "", "不接入中间层的服务名(如 payment),用于演示漏标检测")
	flag.Parse()

	env := services.Start(*rogue)
	go http.ListenAndServe(":19000", monitor.NewHandler(env.Snapshot))

	fmt.Println("仿真电商链路已启动:")
	names := make([]string, 0, len(env.BaseURL))
	for n := range env.BaseURL {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Printf("  %-14s %s\n", n, env.BaseURL[n])
	}
	if *rogue != "" {
		fmt.Printf("⚠️  服务 %s 未接入中间层(漏标演练)\n", *rogue)
	}
	fmt.Println("\n监控大盘: http://127.0.0.1:19000")
	fmt.Println("\n正常流量: curl -s -d '{\"user_id\":1001,\"sku_id\":\"sku-1\",\"qty\":1}' localhost:18001/api/order")
	fmt.Println("压测流量: curl -s -H 'X-Stress-Flag: 1' -d '{\"user_id\":9000001,\"sku_id\":\"sku-1\",\"qty\":1}' localhost:18001/api/order")
	select {}
}
