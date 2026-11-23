package services

import (
	"fmt"
	"strings"

	"stresstest/internal/infra"
	"stresstest/internal/middleware"
	"stresstest/internal/monitor"
)

// Snapshot 汇总监控大盘数据。
func (e *Env) Snapshot() monitor.Snapshot {
	snap := monitor.Snapshot{
		Metrics: middleware.M.Snapshot(),
		Tables:  map[string]int{},
		Topics:  map[string]int{},
	}
	for _, t := range e.DB.Tables() {
		snap.Tables[t] = len(e.DB.Scan(t))
	}
	for _, k := range e.Cache.Keys() {
		if strings.HasPrefix(k, infra.CacheStressPrefix) {
			snap.CacheStressKeys++
		} else {
			snap.CacheRealKeys++
		}
	}
	for _, t := range e.MQ.Topics() {
		snap.Topics[t] = len(e.MQ.Messages(t))
	}
	calls := e.Real3rd.All()
	snap.ThirdRealCalls = len(calls)
	for _, c := range calls {
		if middleware.ScanStressUser(c.Payload) {
			snap.ThirdStressCalls++
		}
	}
	return snap
}

// Pollution 真实环境中的压测数据污染统计(验收时各项必须为零)。
type Pollution struct {
	RealTableRows  int
	RealCacheKeys  int
	RealTopicMsgs  int
	RealThirdCalls int
	Details        []string
}

func (p *Pollution) add(format string, args ...any) {
	p.Details = append(p.Details, fmt.Sprintf(format, args...))
}

// ScanPollution 扫描真实库表、真实缓存、真实主题、真实第三方调用记录。
func (e *Env) ScanPollution() Pollution {
	var p Pollution
	for _, t := range e.DB.Tables() {
		if strings.HasPrefix(t, infra.ShadowPrefix) {
			continue
		}
		for _, row := range e.DB.Scan(t) {
			if infra.HasStressData(row) {
				p.RealTableRows++
				p.add("真实表 %s 含压测数据: %v", t, row)
			}
		}
	}
	for _, k := range e.Cache.Keys() {
		if strings.HasPrefix(k, infra.CacheStressPrefix) {
			continue
		}
		v, _ := e.Cache.RawGet(k)
		if middleware.ScanStressUser(k) || middleware.ScanStressUser(v) {
			p.RealCacheKeys++
			p.add("真实缓存键 %s 含压测数据", k)
		}
	}
	for _, t := range e.MQ.Topics() {
		if strings.HasSuffix(t, infra.ShadowTopicSuffix) {
			continue
		}
		for _, m := range e.MQ.Messages(t) {
			if m.Headers[middleware.StressHeader] == "1" || middleware.ScanStressUser(string(m.Body)) {
				p.RealTopicMsgs++
				p.add("真实主题 %s 含压测消息: %s", t, m.Body)
			}
		}
	}
	for _, c := range e.Real3rd.All() {
		if middleware.ScanStressUser(c.Payload) {
			p.RealThirdCalls++
			p.add("真实第三方 %s 收到压测调用: %s", c.Kind, c.Payload)
		}
	}
	return p
}
