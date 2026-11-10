package kprobe

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	klog "github.com/khan-lau/kutils/klogger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// resetForTest 重置包级状态并从默认注册表摘除探针, 供各用例获得干净初始态
func resetForTest() {
	if p := current.Load(); p != nil {
		prometheus.Unregister(p)
	}
	current.Store(nil)
	stopped.Store(false)
}

// hookReadMemStats 替换内存统计采集函数, 返回恢复函数。
// 前提: 包内测试默认串行执行(无 t.Parallel), 裸写全局变量不构成竞争;
// 若未来引入并行测试, 需先将 readMemStats 改为原子变量
func hookReadMemStats(t *testing.T, fn func(*runtime.MemStats)) {
	t.Helper()
	old := readMemStats
	readMemStats = fn
	t.Cleanup(func() { readMemStats = old })
}

func TestInitValidation(t *testing.T) {
	t.Cleanup(resetForTest)

	if err := Init(Option{}); err == nil || !strings.Contains(err.Error(), "ConfigPath") {
		t.Fatalf("empty ConfigPath should fail, got err=%v", err)
	}

	opt := Option{ConfigPath: "conf/app.toml"}
	if err := Init(opt); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if err := Init(opt); !errors.Is(err, ErrAlreadyInit) {
		t.Fatalf("duplicate Init should return ErrAlreadyInit, got %v", err)
	}
}

func TestMetricsOutput(t *testing.T) {
	t.Cleanup(resetForTest)

	if err := Init(Option{ConfigPath: "conf/app.toml", LogPaths: []string{"logs/app.log"}}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	TopicLag("order-create", TopicLagReceive, 3.2)

	// 挂到独立注册表(Pedantic 会校验 Describe/Collect 一致性)
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(current.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	want := []string{
		"kprobe_mem_heap_alloc_bytes",
		"kprobe_mem_heap_sys_bytes",
		"kprobe_mem_stack_inuse_bytes",
		"kprobe_gc_pause_total_ns",
		"kprobe_gc_pause_last_ns",
		"kprobe_gc_count_total",
		"kprobe_topic_lag_milliseconds",
		"kprobe_service_info",
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("metric %s missing, got %v", name, got)
		}
	}

	// 以 Prometheus 文本格式编码后再解析, 验证输出格式合法
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.Negotiate(http.Header{"Accept": []string{"text/plain"}}))
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	// common v0.66+ 起 TextParser 零值(scheme=unset)会 panic, 须用构造函数显式指定校验方案
	parser := expfmt.NewTextParser(model.UTF8Validation)
	if _, err := parser.TextToMetricFamilies(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("re-parse exposition text: %v", err)
	}

	// topic 延迟与身份指标的值
	val := testutilCollect(t, current.Load(), "kprobe_topic_lag_milliseconds")
	if val != 3.2 {
		t.Fatalf("topic lag = %v, want 3.2", val)
	}
	val = testutilCollect(t, current.Load(), "kprobe_service_info")
	if val != 1 {
		t.Fatalf("service info = %v, want 1", val)
	}
}

// testutilCollect 采集指定指标家族的样本值(仅支持单序列指标)
func testutilCollect(t *testing.T, c prometheus.Collector, name string) float64 {
	return testutilCollectSeries(t, c, name, nil)
}

// testutilCollectSeries 按标签值匹配采集指定序列的样本值(labels 为 nil 时取唯一序列)
func testutilCollectSeries(t *testing.T, c prometheus.Collector, name string, labels map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labels != nil && !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			if m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s series %v not found", name, labels)
	return 0
}

// labelsMatch 序列标签与期望完全一致
func labelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, lp := range got {
		v, ok := want[lp.GetName()]
		if !ok || v != lp.GetValue() {
			return false
		}
	}
	return true
}

func TestCacheModel(t *testing.T) {
	t.Cleanup(resetForTest)

	calls := 0
	hookReadMemStats(t, func(m *runtime.MemStats) {
		calls++
		runtime.ReadMemStats(m)
	})

	if err := Init(Option{ConfigPath: "conf/app.toml", CacheTTL: 50 * time.Millisecond}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	reg := prometheus.NewRegistry()
	if err := reg.Register(current.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 首次抓取(空缓存)触发临时计算
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}
	if calls != 1 {
		t.Fatalf("first gather should compute once, calls=%d", calls)
	}

	// TTL 内多次抓取仅计算一次
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}
	if calls != 1 {
		t.Fatalf("gather within ttl should hit cache, calls=%d", calls)
	}

	// 过期后下次抓取重算并刷新
	time.Sleep(60 * time.Millisecond)
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}
	if calls != 2 {
		t.Fatalf("gather after ttl should recompute, calls=%d", calls)
	}
}

func TestTopicLag(t *testing.T) {
	t.Cleanup(resetForTest)

	if err := Init(Option{ConfigPath: "conf/app.toml", MaxTopics: 3}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	TopicLag("a", TopicLagReceive, 1)
	TopicLag("a", TopicLagReceive, 1.5) // 同 topic 同 type 覆盖
	TopicLag("a", TopicLagProcess, 0.8) // 同 topic 不同 type 独立序列
	if n := current.Load().topics.size(); n != 2 {
		t.Fatalf("known combos = %d, want 2", n)
	}
	wantVals := map[string]float64{"receive": 1.5, "process": 0.8}
	for lagType, want := range wantVals {
		if v := testutilCollectSeries(t, current.Load(), "kprobe_topic_lag_milliseconds",
			map[string]string{"topic": "a", "type": lagType}); v != want {
			t.Fatalf("topic a type=%s = %v, want %v", lagType, v, want)
		}
	}

	TopicLag("b", TopicLagReceive, 2) // 新组合, 未超限
	TopicLag("c", TopicLagReceive, 3) // 超限拒绝, 不影响已上报组合
	if n := current.Load().topics.size(); n != 3 {
		t.Fatalf("known combos = %d, want 3", n)
	}

	TopicLag("", TopicLagReceive, 0) // 空 topic 拒绝
	TopicLag("d", "", 0)             // 空 type 拒绝
	if n := current.Load().topics.size(); n != 3 {
		t.Fatalf("known combos after rejects = %d, want 3", n)
	}
}

func TestTopicLagConcurrent(t *testing.T) {
	t.Cleanup(resetForTest)

	if err := Init(Option{ConfigPath: "conf/app.toml"}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// 多 goroutine 并发上报同一批 topic(混合热路径/慢路径), -race 下验证无数据竞争
	const goroutines, topics = 8, 16
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := range 200 {
				TopicLag(fmt.Sprintf("topic-%d", (seed+i)%topics), TopicLagReceive, float64(i))
			}
		}(g)
	}
	wg.Wait()

	if n := current.Load().topics.size(); n != topics {
		t.Fatalf("known topics = %d, want %d", n, topics)
	}

	// 终检: Gather 后验证全部 topic 序列存在且值合法(非负、非 NaN)
	reg := prometheus.NewRegistry()
	if err := reg.Register(current.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "kprobe_topic_lag_milliseconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "topic" {
					found[lp.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	for i := range topics {
		name := fmt.Sprintf("topic-%d", i)
		v, ok := found[name]
		if !ok {
			t.Fatalf("series %s missing, got %d series", name, len(found))
		}
		if v < 0 || math.IsNaN(v) {
			t.Fatalf("series %s value invalid: %v", name, v)
		}
	}
}

func TestGCLifecycle(t *testing.T) {
	t.Cleanup(resetForTest)

	if err := Init(Option{ConfigPath: "conf/app.toml"}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	TopicLag("a", TopicLagReceive, 1)

	Close()
	Close() // 幂等
	if err := Init(Option{ConfigPath: "conf/app.toml"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Init after Close should return ErrClosed, got %v", err)
	}

	// Close 后指标注册表仍可读
	if v := testutilCollect(t, current.Load(), "kprobe_service_info"); v != 1 {
		t.Fatalf("service info after Close = %v, want 1", v)
	}

	// Close 后 TopicLag 丢弃且无副作用(topic 数不变)
	TopicLag("b", TopicLagReceive, 2)
	if n := current.Load().topics.size(); n != 1 {
		t.Fatalf("known topics after Close = %d, want 1", n)
	}
}

func TestPanicIsolation(t *testing.T) {
	t.Cleanup(resetForTest)

	hookReadMemStats(t, func(m *runtime.MemStats) {
		panic("inject collect panic")
	})

	if err := Init(Option{ConfigPath: "conf/app.toml"}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	reg := prometheus.NewRegistry()
	if err := reg.Register(current.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 采集路径 panic 在 Collect 内 recover, 不向宿主 goroutine 传播
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}
}

func TestLogCallback(t *testing.T) {
	t.Cleanup(resetForTest)

	var mu sync.Mutex
	var msgs []string
	logFn := func(lvl klog.Level, tag string, skipFix int, f string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		msgs = append(msgs, fmt.Sprintf(lvl.String()+"|"+tag+"|"+f, args...))
	}

	// 未注入回调(Init 前): 无实例可记录日志, 静默丢弃且不 panic
	TopicLag("x", TopicLagReceive, 1)

	if err := Init(Option{ConfigPath: "conf/app.toml", MaxTopics: 1, Log: logFn}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	TopicLag("a", TopicLagReceive, 1)
	TopicLag("b", TopicLagReceive, 2) // 超限拒绝 → Warn
	TopicLag("", TopicLagReceive, 3)  // 空 topic → Warn

	// 采集路径 panic → Error 经回调输出
	hookReadMemStats(t, func(m *runtime.MemStats) {
		panic("boom")
	})
	reg := prometheus.NewRegistry()
	if err := reg.Register(current.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}

	// Close 后 TopicLag 丢弃 → Warn
	Close()
	TopicLag("z", TopicLagReceive, 1)

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(msgs, "\n")
	for _, want := range []string{
		"WARNING|kprobe|topic lag dropped, topic=\"b\" type=\"receive\": exceed max topics(1)",
		"WARNING|kprobe|topic lag dropped: empty topic name",
		"ERROR|kprobe|collect panic recovered: boom",
		"WARNING|kprobe|topic lag dropped, topic=\"z\" type=\"receive\": kprobe closed",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("log %q missing, got:\n%s", want, joined)
		}
	}
}
