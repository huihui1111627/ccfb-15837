package middleware

import (
	"sort"
	"sync"
	"time"
)

// EntryStat 每个服务入口的压测流量标记统计
type EntryStat struct {
	StressIn int64 `json:"stress_in"` // 识别为压测流量的请求数
	Marked   int64 `json:"marked"`    // 携带标记的
	Lost     int64 `json:"lost"`      // 标记丢失的
}

// PropStat 某种调用方式(同步/MQ/线程池)的透传统计
type PropStat struct {
	Total int64 `json:"total"`
	OK    int64 `json:"ok"`
}

type LatencyView struct {
	Count     int64   `json:"count"`
	AvgMicros float64 `json:"avg_micros"`
	P99Micros float64 `json:"p99_micros"`
	MaxMicros float64 `json:"max_micros"`
}

type latencyStat struct {
	count   int64
	sum     int64
	max     int64
	samples []int64
}

func (l *latencyStat) add(d time.Duration) {
	n := d.Nanoseconds()
	l.count++
	l.sum += n
	if n > l.max {
		l.max = n
	}
	if len(l.samples) < 2048 {
		l.samples = append(l.samples, n)
	} else {
		l.samples[l.count%2048] = n
	}
}

func (l *latencyStat) view() LatencyView {
	v := LatencyView{Count: l.count}
	if l.count > 0 {
		v.AvgMicros = float64(l.sum) / float64(l.count) / 1e3
		v.MaxMicros = float64(l.max) / 1e3
	}
	if len(l.samples) > 0 {
		s := append([]int64(nil), l.samples...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		idx := int(float64(len(s)) * 0.99)
		if idx >= len(s) {
			idx = len(s) - 1
		}
		v.P99Micros = float64(s[idx]) / 1e3
	}
	return v
}

// LeakEvent 漏标/污染告警
type LeakEvent struct {
	Time      time.Time `json:"time"`
	Component string    `json:"component"`
	Detail    string    `json:"detail"`
}

// Metrics 全局指标与熔断状态(单进程仿真,生产上对应独立控制台)
type Metrics struct {
	mu           sync.Mutex
	entries      map[string]*EntryStat
	propagation  map[string]map[string]*PropStat
	shadowWrites map[string]int64
	shadowMiss   map[string]int64
	mockCalls    map[string]int64
	leaks        []LeakEvent
	latency      map[string]*latencyStat
	breakerOpen  bool
	breakerSince time.Time
	breakerWhy   string
}

var M = NewMetrics()

func NewMetrics() *Metrics {
	return &Metrics{
		entries:      map[string]*EntryStat{},
		propagation:  map[string]map[string]*PropStat{},
		shadowWrites: map[string]int64{},
		shadowMiss:   map[string]int64{},
		mockCalls:    map[string]int64{},
		latency:      map[string]*latencyStat{},
	}
}

func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := NewMetrics()
	m.entries, m.propagation = f.entries, f.propagation
	m.shadowWrites, m.shadowMiss, m.mockCalls = f.shadowWrites, f.shadowMiss, f.mockCalls
	m.latency = f.latency
	m.leaks = nil
	m.breakerOpen, m.breakerWhy, m.breakerSince = false, "", time.Time{}
}

func (m *Metrics) RecordEntry(service string, marked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.entries[service]
	if s == nil {
		s = &EntryStat{}
		m.entries[service] = s
	}
	s.StressIn++
	if marked {
		s.Marked++
	} else {
		s.Lost++
	}
}

func (m *Metrics) RecordPropagation(service, callType string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pm := m.propagation[service]
	if pm == nil {
		pm = map[string]*PropStat{}
		m.propagation[service] = pm
	}
	p := pm[callType]
	if p == nil {
		p = &PropStat{}
		pm[callType] = p
	}
	p.Total++
	if ok {
		p.OK++
	}
}

func (m *Metrics) RecordShadowWrite(table string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shadowWrites[table]++
}

func (m *Metrics) RecordShadowMiss(table string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shadowMiss[table]++
}

func (m *Metrics) RecordMock(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mockCalls[name]++
}

func (m *Metrics) RecordLatency(component string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.latency[component]
	if l == nil {
		l = &latencyStat{}
		m.latency[component] = l
	}
	l.add(d)
}

// RecordLeak 发现压测数据进入真实环境:立即告警并熔断压测。
func (m *Metrics) RecordLeak(component, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaks = append(m.leaks, LeakEvent{Time: time.Now(), Component: component, Detail: detail})
	if !m.breakerOpen {
		m.breakerOpen = true
		m.breakerSince = time.Now()
		m.breakerWhy = "漏标检测: " + component
	}
}

func (m *Metrics) BreakerOpen() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.breakerOpen
}

func (m *Metrics) ResetBreaker() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.breakerOpen, m.breakerWhy, m.breakerSince = false, "", time.Time{}
}

type MetricsSnapshot struct {
	Entries       map[string]EntryStat           `json:"entries"`
	Propagation   map[string]map[string]PropStat `json:"propagation"`
	ShadowWrites  map[string]int64               `json:"shadow_writes"`
	ShadowMiss    map[string]int64               `json:"shadow_miss"`
	MockCalls     map[string]int64               `json:"mock_calls"`
	Leaks         []LeakEvent                    `json:"leaks"`
	Latency       map[string]LatencyView         `json:"latency"`
	BreakerOpen   bool                           `json:"breaker_open"`
	BreakerReason string                         `json:"breaker_reason"`
	BreakerSince  string                         `json:"breaker_since"`
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := MetricsSnapshot{
		Entries:      map[string]EntryStat{},
		Propagation:  map[string]map[string]PropStat{},
		ShadowWrites: map[string]int64{},
		ShadowMiss:   map[string]int64{},
		MockCalls:    map[string]int64{},
		Latency:      map[string]LatencyView{},
	}
	for k, v := range m.entries {
		s.Entries[k] = *v
	}
	for svc, pm := range m.propagation {
		s.Propagation[svc] = map[string]PropStat{}
		for ct, p := range pm {
			s.Propagation[svc][ct] = *p
		}
	}
	for k, v := range m.shadowWrites {
		s.ShadowWrites[k] = v
	}
	for k, v := range m.shadowMiss {
		s.ShadowMiss[k] = v
	}
	for k, v := range m.mockCalls {
		s.MockCalls[k] = v
	}
	for k, v := range m.latency {
		s.Latency[k] = v.view()
	}
	s.Leaks = append(s.Leaks, m.leaks...)
	s.BreakerOpen = m.breakerOpen
	s.BreakerReason = m.breakerWhy
	if !m.breakerSince.IsZero() {
		s.BreakerSince = m.breakerSince.Format("15:04:05.000")
	}
	return s
}
