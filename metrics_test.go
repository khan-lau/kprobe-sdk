package kprobe

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestResolveAbs(t *testing.T) {
	// 基准目录用临时目录, 期望值用 filepath.Join 构造, 断言跨平台一致(分隔符随平台)
	base := t.TempDir()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"relative", "conf/app.toml", filepath.Join(base, "conf", "app.toml")},
		{"dot_relative", "./conf/app.toml", filepath.Join(base, "conf", "app.toml")},
		{"clean_dotdot", filepath.Join(base, "conf", "..", "app.toml"), filepath.Join(base, "app.toml")},
		{"already_abs", filepath.Join(base, "app.toml"), filepath.Join(base, "app.toml")},
	}
	for _, c := range cases {
		if got := resolveAbs(base, c.in); got != c.want {
			t.Fatalf("%s: resolveAbs(%q, %q) = %q, want %q", c.name, base, c.in, got, c.want)
		}
	}
}

func TestGcCountDelta(t *testing.T) {
	cases := []struct {
		name string
		prev uint32
		cur  uint32
		want uint64
	}{
		{"normal", 5, 10, 5},
		{"same", 10, 10, 0},
		{"first", 0, 7, 7},
		{"wrap", 0xFFFFFFFE, 1, 3},
		{"wrap_from_max", 0xFFFFFFFF, 0, 1},
	}
	for _, c := range cases {
		if got := gcCountDelta(c.prev, c.cur); got != c.want {
			t.Fatalf("%s: gcCountDelta(%d, %d) = %d, want %d", c.name, c.prev, c.cur, got, c.want)
		}
	}
}

func TestMemCacheTTLAndWrap(t *testing.T) {
	c := &memCache{ttl: time.Hour}

	numGC := uint32(0xFFFFFFFE)
	hookReadMemStats(t, func(m *runtime.MemStats) {
		m.HeapAlloc = 100
		m.HeapSys = 200
		m.StackInuse = 300
		m.PauseTotalNs = 1000
		m.NumGC = numGC
		m.PauseNs[(m.NumGC+255)%256] = 42
	})

	// 首次: 空缓存触发计算, 累计值 = 0xFFFFFFFE
	s := c.get()
	if s.heapAlloc != 100 || s.heapSys != 200 || s.stackInuse != 300 {
		t.Fatalf("mem snapshot = %+v", s)
	}
	if s.gcCountTotal != uint64(0xFFFFFFFE) {
		t.Fatalf("gcCountTotal = %d, want %d", s.gcCountTotal, uint64(0xFFFFFFFE))
	}
	if s.pauseLastNs != 42 {
		t.Fatalf("pauseLastNs = %d, want 42", s.pauseLastNs)
	}

	// TTL 内命中缓存, 不触发计算
	numGC = 1 // NumGC 回绕
	s = c.get()
	if s.gcCountTotal != uint64(0xFFFFFFFE) {
		t.Fatalf("gcCountTotal within ttl = %d, want %d", s.gcCountTotal, uint64(0xFFFFFFFE))
	}

	// 过期后重算: 回绕补差 +3, 累计值单调
	c.at = time.Now().Add(-2 * time.Hour)
	s = c.get()
	if want := uint64(0xFFFFFFFE) + 3; s.gcCountTotal != want {
		t.Fatalf("gcCountTotal after wrap = %d, want %d", s.gcCountTotal, want)
	}
	if s.pauseLastNs != 42 {
		t.Fatalf("pauseLastNs = %d, want 42", s.pauseLastNs)
	}
}
