// Package middleware 全链路压测流量隔离中间层:
// 流量染色、上下文透传(同步/消息队列/线程池)、漏标统计、熔断。
package middleware

import "context"

type ctxKey int

const (
	ctxKeyStress ctxKey = iota
	ctxKeyTrace
	ctxKeyUser
)

// 透传使用的请求头 / 消息头
const (
	StressHeader = "X-Stress-Flag"
	TraceHeader  = "X-Trace-Id"
	UserHeader   = "X-User-Id"
)

func WithStress(ctx context.Context, v bool) context.Context {
	return context.WithValue(ctx, ctxKeyStress, v)
}

func IsStress(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyStress).(bool)
	return v
}

func WithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyTrace, id)
}

func TraceID(ctx context.Context) string {
	s, _ := ctx.Value(ctxKeyTrace).(string)
	return s
}

func WithUser(ctx context.Context, uid int64) context.Context {
	return context.WithValue(ctx, ctxKeyUser, uid)
}

func UserID(ctx context.Context) int64 {
	v, _ := ctx.Value(ctxKeyUser).(int64)
	return v
}

// IsStressUser 压测流量使用专属用户号段 [9000000, 9999999],
// 漏标检测依赖数据特征(号段)识别"没带标记的压测数据"。
func IsStressUser(uid int64) bool { return uid >= 9000000 && uid <= 9999999 }

// ScanStressUser 在字符串(消息体、缓存键、缓存值、第三方报文)中扫描压测用户号段。
func ScanStressUser(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			continue
		}
		j := i
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j-i == 7 && s[i] == '9' {
			var n int64
			for k := i; k < j; k++ {
				n = n*10 + int64(s[k]-'0')
			}
			if IsStressUser(n) {
				return true
			}
		}
		i = j
	}
	return false
}
