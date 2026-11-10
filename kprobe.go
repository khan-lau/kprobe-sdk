// Package kprobe 提供轻量级进程内探针: 采集宿主服务的内存/GC 指标, 接收业务侧
// Topic 消息延迟上报, 并暴露宿主服务身份信息。
//
// SDK 职责 = 指标采集 + 指标缓存: 业务上报与主动计算的指标值一律存入缓存,
// 指标输出永远读缓存。SDK 不内置任何网络服务, /metrics 端点由宿主服务在自身
// HTTP 服务上挂载 promhttp.Handler() 暴露; 宿主收到抓取请求后从缓存取值,
// 缓存未命中(首次抓取或缓存过期)时临时计算或获取指标再返回。
package kprobe

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	klog "github.com/khan-lau/kutils/klogger"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// kprobeLogTag SDK 内部日志的固定 tag
	kprobeLogTag = "kprobe"

	// defaultCacheTTL 内存/GC 指标缓存有效期默认值, 与 agent 抓取周期对齐
	defaultCacheTTL = 15 * time.Second
	// defaultMaxTopics Topic×类型组合数上限默认值, 防指标基数失控
	defaultMaxTopics = 256
)

// TopicLagType 预定义的延迟类型(kprobe_topic_lag_milliseconds 的 type label 取值),
// 也支持业务自定义字符串; MaxTopics 上限按 topic×type 组合计数
const (
	// TopicLagReceive 接收延迟: 消息产生(或到达 MQ)至业务服务接收到
	TopicLagReceive = "receive"
	// TopicLagProcess 处理延迟: 业务服务接收到处理完成
	TopicLagProcess = "process"
)

// Option Init 的配置项。全部显式注入, SDK 不读取环境变量或配置文件。
type Option struct {
	// ConfigPath 宿主服务配置文件路径, 记入 kprobe_service_info 指标;
	// 相对路径以可执行文件所在目录为基准换算为绝对路径(分隔符随平台)
	ConfigPath string
	// LogPaths 宿主服务日志文件路径(软链), 记入 kprobe_service_info 指标;
	// 每条路径一个序列, 为空时输出一条 log_path 为空串的序列;
	// 相对路径以可执行文件所在目录为基准换算为绝对路径(分隔符随平台)
	LogPaths []string
	// CacheTTL 内存/GC 指标缓存有效期, 未命中时下次抓取临时重算; <=0 时取默认 15s
	CacheTTL time.Duration
	// MaxTopics Topic 数量上限, 超限后拒绝新 topic 上报(不影响已上报 topic); <=0 时取默认 256
	MaxTopics int
	// Log SDK 内部日志回调(kutils klogger 约定), SDK 自身不做任何日志写入,
	// 实际写入由注入方实现; 为 nil 时内部日志静默丢弃
	Log klog.AppLogFuncWithTag
}

var (
	// ErrAlreadyInit 重复 Init
	ErrAlreadyInit = errors.New("kprobe: already initialized")
	// ErrClosed Close 之后拒绝再次 Init
	ErrClosed = errors.New("kprobe: closed")
)

var (
	// current 当前实例; Init 成功后非 nil, Close 后保留(指标仍可读)。
	// 原子指针: TopicLag 热路径无锁读取
	current atomic.Pointer[probe]
	// stopped Close 置位; 拒绝后续 Init 与 TopicLag
	stopped atomic.Bool
)

// Init 初始化探针, 将指标注册到 prometheus 默认注册表(宿主挂载
// promhttp.Handler() 即可输出)。失败返回错误, 宿主可选择忽略继续运行
// (仅损失监控); Close 之后不可再次 Init。
func Init(opt Option) (err error) {
	var p *probe
	defer func() {
		if r := recover(); r != nil {
			// panic 可能发生在 CAS 占位/注册之后, 回滚实例位与注册, 避免
			// 实例位被未注册实例永久占用导致后续 Init 恒返回 ErrAlreadyInit
			if p != nil {
				current.CompareAndSwap(p, nil)
				prometheus.Unregister(p)
			}
			if opt.Log != nil {
				opt.Log(klog.ErrorLevel, kprobeLogTag, 0, "init panic recovered: %v", r)
			}
			err = fmt.Errorf("kprobe: init panic: %v", r)
		}
	}()

	if stopped.Load() {
		return ErrClosed
	}
	if opt.ConfigPath == "" {
		return errors.New("kprobe: ConfigPath is required")
	}

	p = newProbe(opt)
	// CAS 抢占唯一实例位: 并发 Init 只有一个成功
	if !current.CompareAndSwap(nil, p) {
		return ErrAlreadyInit
	}
	if err := prometheus.Register(p); err != nil {
		current.Store(nil) // 注册失败回滚实例位
		if opt.Log != nil {
			opt.Log(klog.ErrorLevel, kprobeLogTag, 0, "register metrics error: %s", err.Error())
			opt.Log(klog.WarnLevel, kprobeLogTag, 0, "init rolled back: reports written to the unregistered instance are dropped")
		}
		return fmt.Errorf("kprobe: register metrics: %w", err)
	}
	// 复查停止标志: 收窄 Close 与 Init 并发时"注册出已停实例"的窗口
	if stopped.Load() {
		prometheus.Unregister(p)
		current.Store(nil)
		if opt.Log != nil {
			opt.Log(klog.WarnLevel, kprobeLogTag, 0, "init rolled back: kprobe closed during init")
		}
		return ErrClosed
	}
	return nil
}

// TopicLag 上报业务 Topic 的指定类型延迟(毫秒), 同 topic 同 type 覆盖旧值(gauge 语义),
// 同名 topic 的不同 type 为独立序列。lagType 建议 TopicLagReceive/TopicLagProcess,
// 也接受业务自定义字符串。超出 MaxTopics 上限(topic×type 组合数)的新组合会被拒绝
// 并打日志, 不报错、不影响已上报组合; 未初始化时静默丢弃(尚无实例可记录日志),
// 已 Close 时丢弃并打日志。
func TopicLag(topic, lagType string, lagMilliseconds float64) {
	// 热路径无锁: 原子读实例指针与停止标志
	p := current.Load()
	if p == nil {
		return
	}
	if stopped.Load() {
		p.log(klog.WarnLevel, "topic lag dropped, topic=%q type=%q: kprobe closed", topic, lagType)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.log(klog.ErrorLevel, "topic lag panic recovered, topic=%q type=%q: %v", topic, lagType, r)
		}
	}()
	p.topics.set(topic, lagType, lagMilliseconds)
}

// Close 停止探针(幂等, 重复调用无副作用): 拒绝后续 Init 与 TopicLag 上报;
// 指标注册表仍可读, 支持宿主退出前最后一次抓取。
func Close() {
	stopped.Store(true)
}

// probe 探针实例, 实现 prometheus.Collector, 以单一 collector 注册到注册表
type probe struct {
	cache  *memCache
	topics *topicSet
	info   *prometheus.GaugeVec
	logf   klog.AppLogFuncWithTag // 注入的日志回调, 为 nil 时丢弃全部内部日志
}

func newProbe(opt Option) *probe {
	ttl := opt.CacheTTL
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	maxTopics := opt.MaxTopics
	if maxTopics <= 0 {
		maxTopics = defaultMaxTopics
	}
	return &probe{
		cache:  &memCache{ttl: ttl},
		topics: newTopicSet(opt.Log, maxTopics),
		info:   newInfoVec(opt.Log, opt.ConfigPath, opt.LogPaths),
		logf:   opt.Log,
	}
}

// Collect 输出全部指标。采集路径中的 panic 在此兜底 recover, 保证探针异常
// 不波及宿主进程(仅输出已采集到的部分指标)。
func (that *probe) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			that.log(klog.ErrorLevel, "collect panic recovered: %v", r)
		}
	}()

	that.collectMem(ch)
	that.topics.vec.Collect(ch)
	that.info.Collect(ch)
}

// Describe 静态声明全部指标描述符, 供注册表做注册校验与输出元数据; 不触发缓存刷新
func (that *probe) Describe(ch chan<- *prometheus.Desc) {
	ch <- descMemHeapAlloc
	ch <- descMemHeapSys
	ch <- descMemStackInuse
	ch <- descGCPauseTotal
	ch <- descGCPauseLast
	ch <- descGCCount
	that.topics.vec.Describe(ch)
	that.info.Describe(ch)
}

// collectMem 输出内存/GC 六项指标
func (that *probe) collectMem(ch chan<- prometheus.Metric) {
	s := that.cache.get()
	ch <- prometheus.MustNewConstMetric(descMemHeapAlloc, prometheus.GaugeValue, s.heapAlloc)
	ch <- prometheus.MustNewConstMetric(descMemHeapSys, prometheus.GaugeValue, s.heapSys)
	ch <- prometheus.MustNewConstMetric(descMemStackInuse, prometheus.GaugeValue, s.stackInuse)
	ch <- prometheus.MustNewConstMetric(descGCPauseTotal, prometheus.CounterValue, float64(s.pauseTotalNs))
	ch <- prometheus.MustNewConstMetric(descGCPauseLast, prometheus.GaugeValue, float64(s.pauseLastNs))
	ch <- prometheus.MustNewConstMetric(descGCCount, prometheus.CounterValue, float64(s.gcCountTotal))
}

// log 日志记录, 会自动添加 kprobeLogTag
//
//go:inline
func (that *probe) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, kprobeLogTag, 1, format, args...)
	}
}
