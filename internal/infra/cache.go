package infra

import (
	"context"
	"sync"
	"time"

	"stresstest/internal/middleware"
)

// ---------------- 仿真缓存(Redis) ----------------

type Cache struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewCache() *Cache { return &Cache{data: map[string]string{}} }

func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.data))
	for k := range c.data {
		out = append(out, k)
	}
	return out
}

func (c *Cache) RawGet(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[key]
	return v, ok
}

// ---------------- 缓存隔离代理:压测键加前缀 ----------------

const CacheStressPrefix = "stress__"

type CacheProxy struct{ c *Cache }

func NewCacheProxy(c *Cache) *CacheProxy { return &CacheProxy{c: c} }

func (p *CacheProxy) key(ctx context.Context, key string) string {
	if middleware.IsStress(ctx) {
		return CacheStressPrefix + key
	}
	return key
}

func (p *CacheProxy) Set(ctx context.Context, key, val string) {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("cache_proxy", time.Since(start)) }()

	if !middleware.IsStress(ctx) && (middleware.ScanStressUser(key) || middleware.ScanStressUser(val)) {
		middleware.M.RecordLeak("cache", "压测数据写入真实缓存键: "+key)
	}
	k := p.key(ctx, key)
	p.c.mu.Lock()
	p.c.data[k] = val
	p.c.mu.Unlock()
}

func (p *CacheProxy) Get(ctx context.Context, key string) (string, bool) {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("cache_proxy", time.Since(start)) }()
	return p.c.RawGet(p.key(ctx, key))
}
