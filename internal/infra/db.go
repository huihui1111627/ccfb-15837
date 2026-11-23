// Package infra 仿真的基础设施:数据库、缓存、消息队列、第三方接口,
// 以及叠加在其上的隔离代理(影子路由、键前缀、影子主题、挡板)。
package infra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"stresstest/internal/middleware"
)

// ---------------- 仿真数据库 ----------------

type Row map[string]any

type Table struct {
	Name string
	mu   sync.Mutex
	Rows []Row
}

type DB struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

func NewDB() *DB { return &DB{tables: map[string]*Table{}} }

func (d *DB) CreateTable(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.tables[name]; !ok {
		d.tables[name] = &Table{Name: name}
	}
}

func (d *DB) HasTable(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.tables[name]
	return ok
}

func (d *DB) Tables() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.tables))
	for n := range d.tables {
		out = append(out, n)
	}
	return out
}

// Scan 返回表的全部行(拷贝),用于验收扫描。
func (d *DB) Scan(name string) []Row {
	d.mu.RLock()
	t := d.tables[name]
	d.mu.RUnlock()
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Row, len(t.Rows))
	copy(out, t.Rows)
	return out
}

// ---------------- 影子库表路由代理 ----------------

const ShadowPrefix = "shadow_"

var ErrShadowNotFound = errors.New("影子表不存在,拒绝写入")

type DBProxy struct{ db *DB }

func NewDBProxy(db *DB) *DBProxy { return &DBProxy{db: db} }

// resolve 影子路由:带标记 -> shadow_ 前缀表;影子表不存在则拒绝,绝不回退真实表。
func (p *DBProxy) resolve(ctx context.Context, table string) (string, error) {
	if middleware.IsStress(ctx) {
		shadow := ShadowPrefix + table
		if !p.db.HasTable(shadow) {
			middleware.M.RecordShadowMiss(table)
			return "", fmt.Errorf("%w: %s", ErrShadowNotFound, shadow)
		}
		return shadow, nil
	}
	return table, nil
}

// HasStressData 数据特征检测:行内是否含压测用户号段。
func HasStressData(row Row) bool {
	for _, v := range row {
		switch vv := v.(type) {
		case int64:
			if middleware.IsStressUser(vv) {
				return true
			}
		case int:
			if middleware.IsStressUser(int64(vv)) {
				return true
			}
		case string:
			if middleware.ScanStressUser(vv) {
				return true
			}
		}
	}
	return false
}

func (p *DBProxy) Insert(ctx context.Context, table string, row Row) error {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("db_proxy", time.Since(start)) }()

	real, err := p.resolve(ctx, table)
	if err != nil {
		return err
	}
	// 漏标检测:未带标记的写请求把压测数据写进真实表 -> 告警并熔断
	if !middleware.IsStress(ctx) && HasStressData(row) {
		middleware.M.RecordLeak("db:"+table, fmt.Sprintf("压测数据写入真实表 %s: %v", table, row))
	}
	t := p.db.table(real)
	t.mu.Lock()
	t.Rows = append(t.Rows, row)
	t.mu.Unlock()
	if middleware.IsStress(ctx) {
		middleware.M.RecordShadowWrite(table)
	}
	return nil
}

func (p *DBProxy) Select(ctx context.Context, table string, pred func(Row) bool) ([]Row, error) {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("db_proxy", time.Since(start)) }()

	real, err := p.resolve(ctx, table)
	if err != nil {
		return nil, err
	}
	t := p.db.table(real)
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []Row
	for _, r := range t.Rows {
		if pred == nil || pred(r) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (p *DBProxy) Update(ctx context.Context, table string, pred func(Row) bool, mutate func(Row)) (int, error) {
	start := time.Now()
	defer func() { middleware.M.RecordLatency("db_proxy", time.Since(start)) }()

	real, err := p.resolve(ctx, table)
	if err != nil {
		return 0, err
	}
	t := p.db.table(real)
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, r := range t.Rows {
		if pred(r) {
			if !middleware.IsStress(ctx) && HasStressData(r) {
				middleware.M.RecordLeak("db:"+table, fmt.Sprintf("未带标记的请求改动了真实表中的压测数据 %s: %v", table, r))
			}
			mutate(r)
			n++
		}
	}
	return n, nil
}

func (d *DB) table(name string) *Table {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tables[name]
}
