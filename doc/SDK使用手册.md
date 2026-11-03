# kprobe SDK 使用手册

kprobe 是轻量级进程内探针 SDK: 采集宿主服务的内存/GC 指标, 接收业务侧 Topic 消息延迟上报, 并暴露宿主服务身份信息。

- **SDK 职责** = 指标采集 + 指标缓存; SDK 不内置任何网络服务, `/metrics` 由宿主在自身 HTTP 服务上挂载 `promhttp.Handler()` 暴露
- **缓存模型** = 写缓存、读缓存: 指标输出永远读缓存, 首次抓取或 TTL 过期时才临时执行 `runtime.ReadMemStats` 计算并刷新缓存, 多路抓取不放大 STW 开销
- **接入成本** = 3 行(import + Init + 挂载 `/metrics`)

## 1. 完整接入范例

宿主服务接入 kprobe 的完整流程(可复制后按需裁剪):

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kprobe"

	"github.com/khan-lau/kutils/klogger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	commonexpfmt "github.com/prometheus/common/expfmt"
)

// wantMetrics 指标清单, 抓取后按此顺序打印
var wantMetrics = []string{
	"kprobe_mem_heap_alloc_bytes",
	"kprobe_mem_heap_sys_bytes",
	"kprobe_mem_stack_inuse_bytes",
	"kprobe_gc_pause_total_ns",
	"kprobe_gc_pause_last_ns",
	"kprobe_gc_count_total",
	"kprobe_topic_lag_seconds",
	"kprobe_service_info",
	"process_start_time_seconds", // client_golang 内置, 平台不支持该采集器时自动缺席
}

func main() {
	// ---- 1. 日志回调: 复用宿主已有的 klogger 日志实现(AppLogFuncWithTag 签名) ----
	lg := klogger.LoggerInstanceOnlyConsole(int8(klogger.InfoLevel))
	defer lg.Sync()
	logf := func(lvl klogger.Level, tag string, skipFix int, f string, args ...any) {
		msg := fmt.Sprintf("[%s] %s", tag, fmt.Sprintf(f, args...))
		switch lvl {
		case klogger.ErrorLevel, klogger.DPanicLevel, klogger.PanicLevel, klogger.FatalLevel:
			lg.KError(skipFix+1, msg) // +1: 闭包自身这一帧
		case klogger.WarnLevel:
			lg.KWarn(skipFix+1, msg)
		case klogger.DebugLevel, klogger.TraceLevel:
			lg.KDebug(skipFix+1, msg)
		default:
			lg.KInfo(skipFix+1, msg)
		}
	}

	// ---- 2. 初始化探针(Log 只允许在 Init 时注入一次, 之后不可更改) ----
	if err := kprobe.Init(kprobe.Option{
		ConfigPath: "conf/app.toml",
		LogPaths:   []string{"logs/app.log"},
		CacheTTL:   15 * time.Second,
		Log:        logf,
	}); err != nil {
		logf(klogger.ErrorLevel, "main", 0, "kprobe init failed: %s", err.Error())
	}

	// ---- 3. 宿主在自身 HTTP 服务上挂载 /metrics(SDK 不内置任何网络服务) ----
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: "127.0.0.1:18080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logf(klogger.ErrorLevel, "main", 0, "http server error: %s", err.Error())
		}
	}()

	// ---- 4. 业务侧周期上报 Topic 消息延迟(模拟业务 goroutine, gauge 同名覆盖) ----
	go func() {
		for lag := 3.2; ; lag += 0.1 {
			kprobe.TopicLag("order-create", lag)
			time.Sleep(5 * time.Second)
		}
	}()

	// ---- 5. 模拟 categraf 周期抓取并打印指标清单中的全部指标 ----
	// 适当的时候 = 抓取时刻: 缓存命中直接返回缓存值; 首次抓取或 TTL 过期才临时执行
	// runtime.ReadMemStats 计算并刷新缓存, 多路抓取不会放大 STW 开销。
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const metricsURL = "http://127.0.0.1:18080/metrics"
	fetchAndPrint(metricsURL) // 启动后立即抓取一次(首次: 缓存未命中, 触发临时计算)

	tick := time.NewTicker(15 * time.Second) // 与 CacheTTL 对齐
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			fetchAndPrint(metricsURL) // TTL 内重复抓取: 直接读缓存
		case <-ctx.Done():
			kprobe.Close() // 宿主退出时调用(幂等), 之后 /metrics 仍可读, 支持最后一次抓取
			return
		}
	}
}

// fetchAndPrint 抓取 /metrics 并按指标清单打印全部指标
func fetchAndPrint(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("fetch %s error: %v\n", url, err)
		return
	}
	defer resp.Body.Close()

	parser := commonexpfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil && len(families) == 0 {
		fmt.Printf("parse metrics error: %v\n", err)
		return
	}

	fmt.Printf("---- scrape %s ----\n", time.Now().Format("15:04:05"))
	for _, name := range wantMetrics {
		mf, ok := families[name]
		if !ok {
			continue // 平台不支持等场景下该指标缺席, 跳过
		}
		for _, m := range mf.Metric {
			var v float64
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				v = m.GetCounter().GetValue()
			default:
				v = m.GetGauge().GetValue()
			}
			fmt.Printf("  %s%s = %s\n", name, formatLabels(m.Label), strconv.FormatFloat(v, 'g', -1, 64))
		}
	}
}

// formatLabels 输出 prometheus 文本格式的 label 部分
func formatLabels(labels []*dto.LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
```

## 2. 分步说明

### 2.1 日志回调构造

SDK 自身不做任何日志写入, 全部内部日志经注入的回调输出(klogger `AppLogFuncWithTag` 签名)。宿主用闭包把回调适配到自有 klogger 实例:

- `skipFix+1`: 闭包自身这一帧也要消耗, 保证日志定位到 SDK 内部真实调用点
- 未注入 `Log`(nil)时, SDK 内部日志静默丢弃, 不影响功能

### 2.2 Init

```go
err := kprobe.Init(kprobe.Option{
    ConfigPath: "conf/app.toml",          // 必填, 记入 kprobe_service_info 指标
    LogPaths:   []string{"logs/app.log"}, // 日志文件路径(软链), 每条路径一个序列
    CacheTTL:   15 * time.Second,         // 缓存有效期, <=0 取默认 15s, 与 agent 抓取周期对齐
    MaxTopics:  256,                      // topic 上限, <=0 取默认 256, 超限拒绝新 topic 上报
    Log:        logf,                     // 一次性注入, 之后不可更改
})
```

- 宿主服务启动时**只调用一次**; 重复调用返回 `ErrAlreadyInit`, `Close` 之后调用返回 `ErrClosed`
- 失败(含 panic)自动回滚实例位与注册, **可重试 Init**; 宿主也可选择忽略失败继续运行(仅损失监控); Init 内部 recover, 不向宿主抛 panic
- **路径换算**: `ConfigPath`/`LogPaths` 中的相对路径以**可执行文件所在目录**为基准换算为绝对路径(不依赖进程工作目录, 守护进程场景 cwd 不确定); 已是绝对路径则仅做清理; 路径分隔符随平台(Windows `\`, Unix `/`), 由 `filepath` 统一处理

### 2.3 挂载 /metrics

```go
mux.Handle("/metrics", promhttp.Handler())
```

复用宿主已有 HTTP 服务的监听地址与端口; kprobe 的指标已注册到 prometheus 默认注册表, 挂载即输出(同时自动附带 `go_*` / `process_*` 内置指标)。

### 2.4 TopicLag 上报

```go
kprobe.TopicLag("order-create", 3.2)
```

- 上报业务 Topic 的消息延迟(秒), gauge 语义, 同名 topic 覆盖旧值
- 每个 topic 一条 `kprobe_topic_lag_seconds{topic="..."}` 序列, 惰性创建
- 超出 `MaxTopics` 的新 topic 被拒绝并打 W 日志, 不报错、不影响已上报 topic; 空 topic 名拒绝
- 未初始化时静默丢弃; 已 `Close` 后丢弃并打日志; 内部 recover, 不向业务侧抛 panic

### 2.5 指标获取时机

| 时机 | 行为 |
|---|---|
| 业务调用 `TopicLag` | 实时写入指标缓存(唯一不由抓取驱动的指标) |
| 抓取 `/metrics` 时, 缓存命中(TTL 内) | 直接返回缓存值, 零计算 |
| 抓取时缓存未命中(首次或 TTL 过期) | 临时执行一次 `runtime.ReadMemStats` 计算内存/GC 指标并刷新缓存 |
| `Close` 之后 | 拒绝新的 Init 与 TopicLag, 但指标注册表仍可读, 支持退出前最后一次抓取 |

抓取侧(或调试)可按上文 `fetchAndPrint` 的方式拉取并解析输出; 夜莺/categraf 按 prometheus 文本协议解析, 无需额外处理。

## 3. 指标清单

| 指标名 | 类型 | 来源 | 说明 |
| ------ | ---- | ---- | ---- |
| `kprobe_mem_heap_alloc_bytes` | Gauge | `runtime.MemStats.HeapAlloc` | 堆内存占用 |
| `kprobe_mem_heap_sys_bytes` | Gauge | `runtime.MemStats.HeapSys` | 堆内存系统申请量 |
| `kprobe_mem_stack_inuse_bytes` | Gauge | `runtime.MemStats.StackInuse` | 栈内存占用 |
| `kprobe_gc_pause_total_ns` | Counter | `runtime.MemStats.PauseTotalNs` | GC 暂停累计时间(NumGC 回绕按 2^32 补差, 保证单调) |
| `kprobe_gc_pause_last_ns` | Gauge | `MemStats.PauseNs[(NumGC+255)%256]` | 最近一次 GC 耗时 |
| `kprobe_gc_count_total` | Counter | `runtime.MemStats.NumGC` | GC 触发次数(频率 = rate) |
| `kprobe_topic_lag_seconds{topic}` | Gauge | SDK API 上报 | 每个 Topic 的消息延迟时间 |
| `kprobe_service_info{exe_path,config_path,log_path}` | Gauge(=1) | Init 注册 | 宿主服务身份信息; 三个路径均为绝对路径(config/log 相对路径按 exe 目录换算, 分隔符随平台) |
| `process_start_time_seconds` | Gauge | client_golang 内置 | 进程启动时刻(平台不支持该采集器时缺席) |

## 4. 验证

```powershell
go build ./...
go vet ./...
go test -race -count=1 -timeout 120s ./...
```

抓取验证(宿主运行中):

```powershell
(Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18080/metrics).Content
```

预期输出包含上文指标清单中全部 `kprobe_*` 序列及 `go_*` / `process_*` 内置指标; `kprobe_service_info` 恒为 1, 可作为 `absent()` 探针失联告警锚点。
