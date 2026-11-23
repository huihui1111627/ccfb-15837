package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

// HTTPMiddleware 服务入口中间件:从请求头恢复压测标记到上下文,
// 并统计每个服务压测流量的标记丢失比例。
func HTTPMiddleware(service string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		marked := r.Header.Get(StressHeader) == "1"
		uid, _ := strconv.ParseInt(r.Header.Get(UserHeader), 10, 64)

		ctx := r.Context()
		if marked {
			ctx = WithStress(ctx, true)
		}
		if t := r.Header.Get(TraceHeader); t != "" {
			ctx = WithTrace(ctx, t)
		}
		if uid != 0 {
			ctx = WithUser(ctx, uid)
		}
		// 漏标检测:请求没带标记,但数据特征(压测用户号段)表明它是压测流量
		if marked || IsStressUser(uid) {
			M.RecordEntry(service, marked)
			M.RecordPropagation(service, "sync", marked)
		}
		M.RecordLatency("http_middleware", time.Since(start))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Inject 把上下文中的标记注入出站请求头,业务代码无感知。
func Inject(ctx context.Context, req *http.Request) {
	if IsStress(ctx) {
		req.Header.Set(StressHeader, "1")
	}
	if t := TraceID(ctx); t != "" {
		req.Header.Set(TraceHeader, t)
	}
	if uid := UserID(ctx); uid != 0 {
		req.Header.Set(UserHeader, strconv.FormatInt(uid, 10))
	}
}

var defaultClient = &http.Client{Timeout: 10 * time.Second}

// Do 替代 http.Client.Do:自动注入标记头。
func Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	Inject(ctx, req)
	return defaultClient.Do(req)
}
