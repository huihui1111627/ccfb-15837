package infra

import (
	"context"
	"sync"
	"time"

	"stresstest/internal/middleware"
)

// ---------------- 仿真第三方(短信/支付/物流网关) ----------------

type ThirdCall struct {
	Kind    string
	Payload string
	At      time.Time
}

// RealThirdParty 真实第三方,记录所有真实到达的调用,供验收检查。
type RealThirdParty struct {
	mu    sync.Mutex
	Calls []ThirdCall
}

func NewRealThirdParty() *RealThirdParty { return &RealThirdParty{} }

func (r *RealThirdParty) record(kind, payload string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Calls = append(r.Calls, ThirdCall{Kind: kind, Payload: payload, At: time.Now()})
}

func (r *RealThirdParty) All() []ThirdCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ThirdCall(nil), r.Calls...)
}

// ---------------- 外部调用挡板 ----------------

type Guard struct {
	name string
	real *RealThirdParty
}

func NewGuard(name string, real *RealThirdParty) *Guard { return &Guard{name: name, real: real} }

// Call 带标记 -> 返回模拟结果,不真正发出;不带标记 -> 真实调用(并做漏标检测)。
func (g *Guard) Call(ctx context.Context, payload string) string {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("thirdparty_guard", time.Since(start)) }()

	if middleware.IsStress(ctx) {
		middleware.M.RecordMock(g.name)
		return "MOCK_OK"
	}
	if middleware.ScanStressUser(payload) {
		middleware.M.RecordLeak("thirdparty:"+g.name, "压测请求打到真实第三方: "+payload)
	}
	g.real.record(g.name, payload)
	return "OK"
}
