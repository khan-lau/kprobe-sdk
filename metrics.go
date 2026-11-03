package kprobe

import (
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// readMemStats 可注入的内存统计采集函数, 生产环境恒为 runtime.ReadMemStats
var readMemStats = runtime.ReadMemStats

// gcCountDelta 计算 NumGC 差值。NumGC 为 uint32 会回绕, 回绕时按 2^32 补差,
// 保证累计值单调。抽为纯函数便于单元测试。
func gcCountDelta(prev, cur uint32) uint64 {
	if cur >= prev {
		return uint64(cur - prev)
	}
	return uint64(^uint32(0)) + 1 - uint64(prev) + uint64(cur)
}

var (
	descMemHeapAlloc  = prometheus.NewDesc("kprobe_mem_heap_alloc_bytes", "堆内存占用(HeapAlloc).", nil, nil)
	descMemHeapSys    = prometheus.NewDesc("kprobe_mem_heap_sys_bytes", "堆内存系统申请量(HeapSys).", nil, nil)
	descMemStackInuse = prometheus.NewDesc("kprobe_mem_stack_inuse_bytes", "栈内存占用(StackInuse).", nil, nil)
	descGCPauseTotal  = prometheus.NewDesc("kprobe_gc_pause_total_ns", "GC 暂停累计时间(纳秒).", nil, nil)
	descGCPauseLast   = prometheus.NewDesc("kprobe_gc_pause_last_ns", "最近一次 GC 暂停耗时(纳秒).", nil, nil)
	descGCCount       = prometheus.NewDesc("kprobe_gc_count_total", "GC 触发次数(频率 = rate).", nil, nil)
)

// memSnapshot 缓存的指标快照, 值拷贝返回, 避免读取期间被并发刷新
type memSnapshot struct {
	heapAlloc    float64
	heapSys      float64
	stackInuse   float64
	pauseTotalNs uint64
	gcCountTotal uint64
	pauseLastNs  uint64
}

// memCache 内存/GC 指标的 TTL 缓存。抓取请求驱动刷新: 未命中(TTL 过期或首次)
// 时临时执行 ReadMemStats 计算并刷新缓存, 无后台 goroutine。
type memCache struct {
	ttl time.Duration

	mu           sync.Mutex
	at           time.Time        // 最近一次刷新时刻; 零值表示尚未刷新
	ms           runtime.MemStats // 最近一次刷新的内存统计
	lastNumGC    uint32           // 上次刷新时的 NumGC, 用于回绕补差
	gcCountTotal uint64           // 自累计的 GC 次数(单调)
	pauseLastNs  uint64           // 最近一次 GC 暂停耗时
}

// get 返回指标快照; 缓存命中直接返回, 未命中时刷新
func (that *memCache) get() memSnapshot {
	that.mu.Lock()
	defer that.mu.Unlock()

	// 缓存命中直接返回
	if !that.at.IsZero() && time.Since(that.at) < that.ttl {
		return memSnapshot{
			heapAlloc:    float64(that.ms.HeapAlloc),
			heapSys:      float64(that.ms.HeapSys),
			stackInuse:   float64(that.ms.StackInuse),
			pauseTotalNs: that.ms.PauseTotalNs,
			gcCountTotal: that.gcCountTotal,
			pauseLastNs:  that.pauseLastNs,
		}
	}

	readMemStats(&that.ms) // 未命中时刷新缓存
	that.gcCountTotal += gcCountDelta(that.lastNumGC, that.ms.NumGC)
	that.lastNumGC = that.ms.NumGC
	that.pauseLastNs = uint64(that.ms.PauseNs[(that.ms.NumGC+255)%256])
	that.at = time.Now()

	return memSnapshot{
		heapAlloc:    float64(that.ms.HeapAlloc),
		heapSys:      float64(that.ms.HeapSys),
		stackInuse:   float64(that.ms.StackInuse),
		pauseTotalNs: that.ms.PauseTotalNs,
		gcCountTotal: that.gcCountTotal,
		pauseLastNs:  that.pauseLastNs,
	}
}
