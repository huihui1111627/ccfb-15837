// Package monitor 压测监控台:指标 API 与链路拓扑大盘页面。
package monitor

import (
	"encoding/json"
	"net/http"
	"time"

	"stresstest/internal/middleware"
)

type Snapshot struct {
	GeneratedAt      string                     `json:"generated_at"`
	Metrics          middleware.MetricsSnapshot `json:"metrics"`
	Tables           map[string]int             `json:"tables"` // 表名 -> 行数(含影子表)
	CacheRealKeys    int                        `json:"cache_real_keys"`
	CacheStressKeys  int                        `json:"cache_stress_keys"`
	Topics           map[string]int             `json:"topics"` // 主题 -> 消息数(含影子主题)
	ThirdRealCalls   int                        `json:"third_real_calls"`
	ThirdStressCalls int                        `json:"third_stress_calls"` // 真实第三方收到的压测调用(必须为 0)
}

func NewHandler(fetch func() Snapshot) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, r *http.Request) {
		s := fetch()
		s.GeneratedAt = time.Now().Format("15:04:05")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})
	return mux
}
