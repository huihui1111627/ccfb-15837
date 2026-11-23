package infra

import (
	"context"
	"strconv"
	"sync"
	"time"

	"stresstest/internal/middleware"
)

// ---------------- 仿真消息队列 ----------------

type Message struct {
	Topic   string
	Headers map[string]string
	Body    []byte
	At      time.Time
}

type MQ struct {
	mu        sync.Mutex
	messages  map[string][]Message
	consumers map[string][]func(Message)
	wg        sync.WaitGroup
}

func NewMQ() *MQ {
	return &MQ{messages: map[string][]Message{}, consumers: map[string][]func(Message){}}
}

func (m *MQ) Subscribe(topic string, h func(Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumers[topic] = append(m.consumers[topic], h)
}

func (m *MQ) PublishRaw(topic string, msg Message) {
	m.mu.Lock()
	m.messages[topic] = append(m.messages[topic], msg)
	hs := append([]func(Message){}, m.consumers[topic]...)
	m.mu.Unlock()
	for _, h := range hs {
		m.wg.Add(1)
		go func(h func(Message)) {
			defer m.wg.Done()
			h(msg)
		}(h)
	}
}

func (m *MQ) Topics() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.messages))
	for t := range m.messages {
		out = append(out, t)
	}
	return out
}

func (m *MQ) Messages(topic string) []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Message(nil), m.messages[topic]...)
}

// Drain 等待已派发消息消费完毕。
func (m *MQ) Drain(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ---------------- MQ 隔离代理:影子主题 ----------------

const ShadowTopicSuffix = "__stress"

type MQProxy struct{ mq *MQ }

func NewMQProxy(mq *MQ) *MQProxy { return &MQProxy{mq: mq} }

// Produce 带标记 -> 影子主题,标记写入消息头;不带标记但含压测数据 -> 漏标告警。
func (p *MQProxy) Produce(ctx context.Context, topic string, body []byte) {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("mq_proxy", time.Since(start)) }()

	headers := map[string]string{}
	t := topic
	if middleware.IsStress(ctx) {
		t = topic + ShadowTopicSuffix
		headers[middleware.StressHeader] = "1"
		if tr := middleware.TraceID(ctx); tr != "" {
			headers[middleware.TraceHeader] = tr
		}
		if uid := middleware.UserID(ctx); uid != 0 {
			headers[middleware.UserHeader] = strconv.FormatInt(uid, 10)
		}
	} else if middleware.ScanStressUser(string(body)) {
		middleware.M.RecordLeak("mq:"+topic, "压测消息发送到真实主题 "+topic+": "+string(body))
	}
	p.mq.PublishRaw(t, Message{Topic: t, Headers: headers, Body: body, At: time.Now()})
}

// Consume 同时订阅真实主题与影子主题;消费影子主题时自动从消息头恢复压测上下文。
func (p *MQProxy) Consume(service, topic string, h func(context.Context, Message)) {
	p.mq.Subscribe(topic, func(m Message) {
		ctx := context.Background()
		if t := m.Headers[middleware.TraceHeader]; t != "" {
			ctx = middleware.WithTrace(ctx, t)
		}
		if middleware.ScanStressUser(string(m.Body)) {
			middleware.M.RecordLeak("mq-consume:"+topic, "真实主题消费者收到压测消息: "+string(m.Body))
		}
		h(ctx, m)
	})
	p.mq.Subscribe(topic+ShadowTopicSuffix, func(m Message) {
		ctx := middleware.WithStress(context.Background(), true)
		if t := m.Headers[middleware.TraceHeader]; t != "" {
			ctx = middleware.WithTrace(ctx, t)
		}
		if uid, err := strconv.ParseInt(m.Headers[middleware.UserHeader], 10, 64); err == nil {
			ctx = middleware.WithUser(ctx, uid)
		}
		middleware.M.RecordPropagation(service, "mq", true)
		h(ctx, m)
	})
}
