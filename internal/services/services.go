// Package services 仿真电商链路:gateway -> order -> user/risk/coupon/inventory/payment
// -> notification(同步+MQ),logistics(MQ),线程池异步审计,定时关单任务。
package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"stresstest/internal/infra"
	"stresstest/internal/middleware"
)

var traceSeq int64
var orderSeq int64 = 100000

type CreateOrderReq struct {
	UserID int64  `json:"user_id"`
	SKU    string `json:"sku_id"`
	Qty    int    `json:"qty"`
}

type Env struct {
	DB       *infra.DB
	DBP      *infra.DBProxy
	Cache    *infra.Cache
	CacheP   *infra.CacheProxy
	MQ       *infra.MQ
	MQP      *infra.MQProxy
	Real3rd  *infra.RealThirdParty
	SMS      *infra.Guard
	PayGW    *infra.Guard
	LogiGW   *infra.Guard
	Executor *middleware.Executor
	Rogue    string // 不接入中间层的服务名,用于漏标演练

	servers   []*http.Server
	schedStop chan struct{}
	BaseURL   map[string]string
}

var ports = map[string]int{
	"gateway": 18001, "user": 18002, "order": 18003, "inventory": 18004,
	"payment": 18005, "notification": 18006, "coupon": 18007, "risk": 18008,
}

var tables = []string{
	"users", "orders", "inventory", "coupon_usage",
	"payments", "notify_log", "shipments", "audit_log",
}

// Start 启动整条仿真链路。rogue 非空时该服务不接入中间层(模拟漏接)。
func Start(rogue string) *Env {
	e := &Env{Rogue: rogue, schedStop: make(chan struct{})}
	e.DB = infra.NewDB()
	e.DBP = infra.NewDBProxy(e.DB)
	e.Cache = infra.NewCache()
	e.CacheP = infra.NewCacheProxy(e.Cache)
	e.MQ = infra.NewMQ()
	e.MQP = infra.NewMQProxy(e.MQ)
	e.Real3rd = infra.NewRealThirdParty()
	e.SMS = infra.NewGuard("sms", e.Real3rd)
	e.PayGW = infra.NewGuard("payment_gw", e.Real3rd)
	e.LogiGW = infra.NewGuard("logistics_gw", e.Real3rd)
	e.Executor = middleware.NewExecutor("order", 4, 4096)

	for _, t := range tables {
		e.DB.CreateTable(t)
		e.DB.CreateTable(infra.ShadowPrefix + t) // 影子表迁移
	}
	e.seed()

	e.BaseURL = map[string]string{}
	for name, p := range ports {
		e.BaseURL[name] = fmt.Sprintf("http://127.0.0.1:%d", p)
	}

	e.registerConsumers()
	e.startHTTPServers()
	e.startScheduler()
	return e
}

func (e *Env) Stop() {
	close(e.schedStop)
	e.Executor.Close()
	for _, s := range e.servers {
		s.Close()
	}
}

func (e *Env) seed() {
	ctx := context.Background()
	e.DBP.Insert(ctx, "inventory", infra.Row{"sku_id": "sku-1", "stock": int64(1000000)})
	for uid := int64(1000); uid < 1010; uid++ {
		e.DBP.Insert(ctx, "users", infra.Row{"user_id": uid, "name": fmt.Sprintf("user-%d", uid), "level": "gold"})
	}
}

// wrap 接入中间层;rogue 服务跳过(模拟未接入的服务)。
func (e *Env) wrap(name string, h http.Handler) http.Handler {
	if e.Rogue == name {
		return h
	}
	return middleware.HTTPMiddleware(name, h)
}

func (e *Env) serve(name string, h http.Handler) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", ports[name]))
	if err != nil {
		log.Fatalf("listen %s: %v", name, err)
	}
	srv := &http.Server{Handler: e.wrap(name, h)}
	e.servers = append(e.servers, srv)
	go srv.Serve(ln)
}

func (e *Env) startHTTPServers() {
	e.serve("gateway", e.gatewayMux())
	e.serve("user", e.userMux())
	e.serve("order", e.orderMux())
	e.serve("inventory", e.inventoryMux())
	e.serve("payment", e.paymentMux())
	e.serve("notification", e.notificationMux())
	e.serve("coupon", e.couponMux())
	e.serve("risk", e.riskMux())
}

// call 服务间同步调用:标记随上下文自动注入请求头。
func (e *Env) call(ctx context.Context, service, path string, body any) ([]byte, int, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.BaseURL[service]+path, bytes.NewReader(b))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := middleware.Do(ctx, req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

func decode(r *http.Request, v any) {
	json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ---------------- gateway:流量入口,染色 + 熔断检查 ----------------

func (e *Env) gatewayMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/order", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// 熔断器打开时,压测流量在入口直接拒绝
		if middleware.IsStress(ctx) && middleware.M.BreakerOpen() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "stress traffic fused by circuit breaker"})
			return
		}
		var req CreateOrderReq
		decode(r, &req)
		ctx = middleware.WithTrace(ctx, fmt.Sprintf("trace-%d", atomic.AddInt64(&traceSeq, 1)))
		ctx = middleware.WithUser(ctx, req.UserID)
		b, code, err := e.call(ctx, "order", "/order/create", req)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		w.Write(b)
	})
	return mux
}

// ---------------- order:下单编排(同步调用 + 线程池 + MQ) ----------------

func (e *Env) orderMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /order/create", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req CreateOrderReq
		decode(r, &req)
		fail := func(code int, msg string) { writeJSON(w, code, map[string]any{"error": msg}) }

		if _, code, err := e.call(ctx, "user", "/user/get", map[string]any{"user_id": req.UserID}); err != nil || code != 200 {
			fail(502, "user service unavailable")
			return
		}
		if _, code, _ := e.call(ctx, "risk", "/risk/check", map[string]any{"user_id": req.UserID}); code != 200 {
			fail(403, "risk rejected")
			return
		}
		e.call(ctx, "coupon", "/coupon/use", map[string]any{"user_id": req.UserID, "sku_id": req.SKU})

		orderID := atomic.AddInt64(&orderSeq, 1)
		amount := int64(req.Qty) * 100
		if err := e.DBP.Insert(ctx, "orders", infra.Row{
			"order_id": orderID, "user_id": req.UserID, "sku_id": req.SKU,
			"qty": req.Qty, "amount": amount, "status": "created", "created_at": time.Now().Unix(),
		}); err != nil {
			fail(500, err.Error())
			return
		}
		if _, code, err := e.call(ctx, "inventory", "/inventory/deduct",
			map[string]any{"user_id": req.UserID, "sku_id": req.SKU, "qty": req.Qty}); err != nil || code != 200 {
			fail(502, "inventory deduct failed")
			return
		}
		if _, code, err := e.call(ctx, "payment", "/payment/pay",
			map[string]any{"user_id": req.UserID, "order_id": orderID, "amount": amount}); err != nil || code != 200 {
			fail(502, "payment failed")
			return
		}

		// 线程池异步任务:写审计日志,上下文自动透传到工作线程
		e.Executor.Submit(ctx, func(c context.Context) {
			e.DBP.Insert(c, "audit_log", infra.Row{
				"order_id": orderID, "user_id": req.UserID, "action": "order_created", "at": time.Now().Unix(),
			})
		})

		// MQ 异步:订单创建事件
		evt, _ := json.Marshal(map[string]any{"order_id": orderID, "user_id": req.UserID, "sku_id": req.SKU, "qty": req.Qty})
		e.MQP.Produce(ctx, "order_created", evt)

		writeJSON(w, 200, map[string]any{"order_id": orderID, "status": "created"})
	})
	return mux
}

// ---------------- user:缓存 + 库 ----------------

func (e *Env) userMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /user/get", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID int64 `json:"user_id"`
		}
		decode(r, &req)
		key := fmt.Sprintf("user:%d", req.UserID)
		if v, ok := e.CacheP.Get(ctx, key); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(v))
			return
		}
		rows, _ := e.DBP.Select(ctx, "users", func(row infra.Row) bool { return row["user_id"] == req.UserID })
		var profile map[string]any
		if len(rows) > 0 {
			profile = map[string]any{"user_id": rows[0]["user_id"], "name": rows[0]["name"], "level": rows[0]["level"]}
		} else {
			profile = map[string]any{"user_id": req.UserID, "name": fmt.Sprintf("user-%d", req.UserID), "level": "normal"}
			e.DBP.Insert(ctx, "users", infra.Row(profile))
		}
		b, _ := json.Marshal(profile)
		e.CacheP.Set(ctx, key, string(b))
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	return mux
}

// ---------------- risk ----------------

func (e *Env) riskMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /risk/check", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID int64 `json:"user_id"`
		}
		decode(r, &req)
		key := fmt.Sprintf("risk:%d", req.UserID)
		if _, ok := e.CacheP.Get(ctx, key); !ok {
			e.CacheP.Set(ctx, key, "pass")
		}
		writeJSON(w, 200, map[string]any{"pass": true})
	})
	return mux
}

// ---------------- coupon ----------------

func (e *Env) couponMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /coupon/use", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID int64  `json:"user_id"`
			SKU    string `json:"sku_id"`
		}
		decode(r, &req)
		e.DBP.Insert(ctx, "coupon_usage", infra.Row{
			"user_id": req.UserID, "coupon_id": fmt.Sprintf("c-%d", req.UserID%5), "sku_id": req.SKU, "at": time.Now().Unix(),
		})
		writeJSON(w, 200, map[string]any{"used": true})
	})
	return mux
}

// ---------------- inventory ----------------

func (e *Env) inventoryMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /inventory/deduct", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID int64  `json:"user_id"`
			SKU    string `json:"sku_id"`
			Qty    int    `json:"qty"`
		}
		decode(r, &req)
		var remaining int64
		e.DBP.Update(ctx, "inventory",
			func(row infra.Row) bool { return row["sku_id"] == req.SKU },
			func(row infra.Row) {
				row["stock"] = row["stock"].(int64) - int64(req.Qty)
				remaining = row["stock"].(int64)
			})
		e.CacheP.Set(ctx, "stock:"+req.SKU, fmt.Sprint(remaining))
		writeJSON(w, 200, map[string]any{"ok": true, "remaining": remaining})
	})
	return mux
}

// ---------------- payment:第三方支付挡板 + 同步通知 ----------------

func (e *Env) paymentMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payment/pay", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  int64 `json:"user_id"`
			OrderID int64 `json:"order_id"`
			Amount  int64 `json:"amount"`
		}
		decode(r, &req)
		e.CacheP.Set(ctx, fmt.Sprintf("idem:pay:%d:%d", req.UserID, req.OrderID), "1")
		ret := e.PayGW.Call(ctx, fmt.Sprintf(`{"user_id":%d,"order_id":%d,"amount":%d}`, req.UserID, req.OrderID, req.Amount))
		e.DBP.Insert(ctx, "payments", infra.Row{
			"order_id": req.OrderID, "user_id": req.UserID, "amount": req.Amount,
			"channel": "mockpay", "result": ret, "at": time.Now().Unix(),
		})
		e.call(ctx, "notification", "/notify/send", map[string]any{
			"user_id": req.UserID, "channel": "sms", "content": fmt.Sprintf("订单 %d 支付成功", req.OrderID),
		})
		writeJSON(w, 200, map[string]any{"paid": true, "gw": ret})
	})
	return mux
}

// ---------------- notification:短信挡板 ----------------

func (e *Env) notificationMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notify/send", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  int64  `json:"user_id"`
			Channel string `json:"channel"`
			Content string `json:"content"`
		}
		decode(r, &req)
		e.sendNotify(ctx, req.UserID, req.Channel, req.Content)
		writeJSON(w, 200, map[string]any{"sent": true})
	})
	return mux
}

func (e *Env) sendNotify(ctx context.Context, uid int64, channel, content string) {
	ret := e.SMS.Call(ctx, fmt.Sprintf(`{"user_id":%d,"channel":%q,"content":%q}`, uid, channel, content))
	e.DBP.Insert(ctx, "notify_log", infra.Row{
		"user_id": uid, "channel": channel, "content": content, "result": ret, "at": time.Now().Unix(),
	})
}

// ---------------- MQ 消费者 ----------------

func (e *Env) registerConsumers() {
	type orderEvent struct {
		OrderID int64  `json:"order_id"`
		UserID  int64  `json:"user_id"`
		SKU     string `json:"sku_id"`
		Qty     int    `json:"qty"`
	}
	// 通知消费者:下单成功发短信
	e.MQP.Consume("notification", "order_created", func(ctx context.Context, m infra.Message) {
		var evt orderEvent
		json.Unmarshal(m.Body, &evt)
		e.sendNotify(ctx, evt.UserID, "sms", fmt.Sprintf("订单 %d 已创建", evt.OrderID))
	})
	// 物流消费者:创建运单,调第三方物流
	e.MQP.Consume("logistics", "order_created", func(ctx context.Context, m infra.Message) {
		var evt orderEvent
		json.Unmarshal(m.Body, &evt)
		ret := e.LogiGW.Call(ctx, fmt.Sprintf(`{"user_id":%d,"order_id":%d,"sku":%q}`, evt.UserID, evt.OrderID, evt.SKU))
		e.DBP.Insert(ctx, "shipments", infra.Row{
			"order_id": evt.OrderID, "user_id": evt.UserID, "status": "shipped", "result": ret, "at": time.Now().Unix(),
		})
	})
	// 通知消费者:超时关单提醒
	e.MQP.Consume("notification", "order_timeout", func(ctx context.Context, m infra.Message) {
		var evt orderEvent
		json.Unmarshal(m.Body, &evt)
		e.sendNotify(ctx, evt.UserID, "sms", fmt.Sprintf("订单 %d 已超时关闭", evt.OrderID))
	})
}

// ---------------- 定时任务:超时关单 ----------------

func (e *Env) startScheduler() {
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-e.schedStop:
				return
			case <-t.C:
				// 定时任务无入口流量,不带标记,只会扫描真实表 —— 影子数据天然隔离
				ctx := context.Background()
				cutoff := time.Now().Unix() - 4
				rows, err := e.DBP.Select(ctx, "orders", func(r infra.Row) bool {
					return r["status"] == "created" && r["created_at"].(int64) < cutoff
				})
				if err != nil {
					continue
				}
				for _, row := range rows {
					oid := row["order_id"].(int64)
					uid := row["user_id"].(int64)
					e.DBP.Update(ctx, "orders",
						func(r infra.Row) bool { return r["order_id"] == oid },
						func(r infra.Row) { r["status"] = "timeout" })
					evt, _ := json.Marshal(map[string]any{"order_id": oid, "user_id": uid})
					e.MQP.Produce(ctx, "order_timeout", evt)
				}
			}
		}
	}()
}
