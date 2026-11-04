# kprobe

轻量级进程内探针 SDK: 采集宿主服务的内存/GC 指标, 接收业务侧 Topic 消息延迟上报, 并暴露宿主服务身份信息。

## 定位与职责

```
宿主服务进程
├── 业务代码 ──TopicLag()──┐
│                          ▼
├── kprobe SDK(指标采集 + 指标缓存) ──注册──> prometheus 默认注册表
│                                              ▲
└── 宿主 HTTP 服务 ──挂载 promhttp.Handler()──┘
                                     │
                    categraf prometheus 插件周期拉取 ──> 夜莺(n9e)
```

- **SDK 职责** = 指标采集 + 指标缓存; 不内置任何网络服务, `/metrics` 复用宿主自身 HTTP 服务端口
- **缓存模型** = 写缓存、读缓存: 指标输出永远读缓存, 首次抓取或 TTL(默认 15s)过期时才临时执行 `runtime.ReadMemStats` 计算并刷新缓存, 多路抓取不放大 STW 开销, 无后台 goroutine
- **稳定性** = `Init`/`TopicLag` 内部 recover 不向宿主抛 panic; `Init` 失败宿主可选择忽略继续运行(仅损失监控)
- **依赖** = 仅 `prometheus/client_golang` 与 `khan-lau/kutils` 的 klogger(仅日志回调类型), 不引入任何 MQ 客户端依赖

## 快速接入(3 行)

```go
import (
    "kprobe"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

err := kprobe.Init(kprobe.Option{ConfigPath: "conf/app.toml"}) // 1. 初始化
mux.Handle("/metrics", promhttp.Handler())                     // 2. 宿主挂载 /metrics
kprobe.TopicLag("order-create", kprobe.TopicLagReceive, 3.2)     // 3. 业务侧上报 Topic 延迟
```

完整接入范例(日志回调构造、Option 全项、抓取与打印)见 [SDK 使用手册](doc/SDK使用手册.md)。

## API

| API | 说明 |
| --- | --- |
| `Init(opt Option) error` | 初始化并注册指标, 启动时调用一次; 重复调用返回 `ErrAlreadyInit`, `Close` 后返回 `ErrClosed`; 失败(含 panic)自动回滚, 可重试, 也可忽略继续运行 |
| `TopicLag(topic, lagType string, lagSeconds float64)` | 上报 Topic 指定类型延迟(秒), `lagType` 用预定义 `TopicLagReceive`/`TopicLagProcess` 或业务自定义; gauge 同覆盖; 超出 `MaxTopics`(默认 256, 按 topic×type 组合计数)的新组合拒绝并打日志, 不报错; 多 goroutine 并发调用安全(热路径无锁) |
| `Close()` | 停止探针(幂等): 拒绝后续 Init 与 TopicLag; 指标注册表仍可读, 支持退出前最后一次抓取 |

**Option 关键项**: `ConfigPath`(必填)/`LogPaths`——相对路径以可执行文件所在目录为基准换算为绝对路径(分隔符随平台); `CacheTTL`(默认 15s); `MaxTopics`(默认 256); `Log`——klogger `AppLogFuncWithTag` 日志回调, **仅允许 Init 时一次性注入, 之后不可更改**, 未注入则内部日志静默丢弃。

## 指标清单

| 指标名 | 类型 | 说明 |
| ------ | ---- | ---- |
| `kprobe_mem_heap_alloc_bytes` | Gauge | 堆内存占用(HeapAlloc) |
| `kprobe_mem_heap_sys_bytes` | Gauge | 堆内存系统申请量(HeapSys) |
| `kprobe_mem_stack_inuse_bytes` | Gauge | 栈内存占用(StackInuse) |
| `kprobe_gc_pause_total_ns` | Counter | GC 暂停累计(NumGC 回绕按 2^32 补差, 保证单调) |
| `kprobe_gc_pause_last_ns` | Gauge | 最近一次 GC 暂停耗时 |
| `kprobe_gc_count_total` | Counter | GC 触发次数(频率 = rate) |
| `kprobe_topic_lag_seconds{topic,type}` | Gauge | 业务 Topic 各类型(接收/处理)延迟(唯一由业务调用实时写入的指标) |
| `kprobe_service_info{exe_path,config_path,log_path}` | Gauge(=1) | 服务身份信息, 三路径均为绝对路径; 可作 `absent()` 探针失联告警锚点 |

另自动附带 client_golang 内置的 `go_*` / `process_*` 指标。

## 指标归属: 上游如何区分服务

服务区分在 SDK 之外完成, SDK 零服务感知:

1. **抓取层(服务级)**: categraf prometheus 插件每个接入服务一条 url, 抓取时 `url_label_key = "instance"` 把 `ip:port` 写进每条序列——SDK 复用宿主端口, 同机端口天然唯一
2. **上报层(主机级)**: categraf 统一附加 `agent_hostname` 与 `[global.labels]`, 即夜莺 ident
3. **身份锚点**: `kprobe_service_info` 的 exe/config/log 绝对路径, 供看板核对与 klogapi 白名单生成

到达夜莺的序列形如: `kprobe_mem_heap_alloc_bytes{instance="10.0.0.5:8080", agent_hostname="node1-10.0.0.5", ...}`。

## 目录结构

```
kprobe/
├── kprobe.go        // 对外 API: Init/TopicLag/Close, probe Collector 与生命周期
├── metrics.go       // 内存/GC 指标: TTL 缓存、NumGC 回绕补差、采集回调
├── topics.go        // Topic 延迟: GaugeVec 惰性 label 与 topic 数量治理
├── info.go          // kprobe_service_info 身份指标(exe/config/log 绝对路径换算)
├── doc/
│   └── SDK使用手册.md // 完整接入范例与分步说明
└── go.mod
```

## 验证

```powershell
go build ./...
go vet ./...
go test -race -count=1 -timeout 120s ./...
```

## 相关文档

- [SDK 使用手册](doc/SDK使用手册.md) —— 完整接入范例、分步说明、验证方式

