package kprobe

import (
	"sync"

	klog "github.com/khan-lau/kutils/klogger"
	"github.com/prometheus/client_golang/prometheus"
)

// topicSet Topic 延迟指标集: GaugeVec 以 {topic, type} 双标签惰性生成序列,
// 以 topic×type 组合数上限治理, 防异常调用方撑爆指标基数; 不再消费的
// topic gauge 保留(语义为最近一次上报值)。
//
// 访问模型: 上报高频写、抓取低频读。热路径(已上报组合)走 sync.Map 原子读 +
// Gauge.Set 原子写, 全程无锁, 多 goroutine 上报零争用; 仅新 topic×type 组合
// 首次上报才进入慢路径互斥锁(校验组合数上限并创建子 gauge 句柄)。
type topicSet struct {
	vec    *prometheus.GaugeVec
	gauges sync.Map // topic+"\x00"+lagType -> prometheus.Gauge, 热路径无锁句柄缓存
	mu     sync.Mutex
	count  int // 已上报 topic×type 组合数, 仅慢路径持有 mu 读写
	max    int
	logf   klog.AppLogFuncWithTag // 注入的日志回调, 为 nil 时丢弃日志
}

func newTopicSet(logf klog.AppLogFuncWithTag, max int) *topicSet {
	return &topicSet{
		vec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kprobe_topic_lag_milliseconds",
			Help: "业务 Topic 的消息延迟时间(毫秒).",
		}, []string{"topic", "type"}),
		max:  max,
		logf: logf,
	}
}

// set 上报单个 topic 指定类型的延迟; 空 topic/type 或超限的新组合拒绝并打日志
func (t *topicSet) set(topic, lagType string, lagMilliseconds float64) {
	if topic == "" {
		t.warn("topic lag dropped: empty topic name")
		return
	}
	if lagType == "" {
		t.warn("topic lag dropped, topic=%q: empty lag type", topic)
		return
	}

	// 热路径: 已建句柄的 topic×type 组合, 原子读 + 原子写, 无锁
	if v, ok := t.gauges.Load(topic + "\x00" + lagType); ok {
		v.(prometheus.Gauge).Set(lagMilliseconds)
		return
	}
	t.addSlow(topic, lagType, lagMilliseconds)
}

// addSlow 慢路径: 新 topic×type 组合首次上报, 互斥校验组合数上限并创建缓存句柄
func (t *topicSet) addSlow(topic, lagType string, lagMilliseconds float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// double-check: 并发下另一 goroutine 可能已完成创建
	if v, ok := t.gauges.Load(topic + "\x00" + lagType); ok {
		v.(prometheus.Gauge).Set(lagMilliseconds)
		return
	}
	if t.count >= t.max {
		t.warn("topic lag dropped, topic=%q type=%q: exceed max topics(%d)", topic, lagType, t.max)
		return
	}
	g := t.vec.WithLabelValues(topic, lagType)
	t.gauges.Store(topic+"\x00"+lagType, g)
	t.count++
	g.Set(lagMilliseconds)
}

// size 已上报 topic×type 组合数(测试与治理观测用)
func (t *topicSet) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.count
}

// warn 日志记录, 会自动添加 kprobeLogTag
func (t *topicSet) warn(format string, args ...any) {
	if t.logf != nil {
		t.logf(klog.WarnLevel, kprobeLogTag, 0, format, args...)
	}
}
