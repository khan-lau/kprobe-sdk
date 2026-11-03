package kprobe

import (
	"os"
	"path/filepath"

	klog "github.com/khan-lau/kutils/klogger"
	"github.com/prometheus/client_golang/prometheus"
)

// newInfoVec 构建宿主服务身份指标: 每条日志路径一个序列, 恒为 1。
// 相对路径(config/log)以可执行文件所在目录为基准换算为绝对路径,
// 不依赖进程工作目录(守护进程场景 cwd 不确定); 各平台路径分隔符由
// filepath 处理(Windows 为 `\`, Unix 为 `/`)。
func newInfoVec(logf klog.AppLogFuncWithTag, configPath string, logPaths []string) *prometheus.GaugeVec {
	exePath, err := os.Executable()
	if err != nil {
		if logf != nil {
			logf(klog.WarnLevel, kprobeLogTag, 0, "resolve executable path failed: %v, fallback to os.Args[0]", err)
		}
		exePath = os.Args[0]
	}
	// os.Args[0] 兜底时可能为相对路径, 统一转为绝对路径
	if abs, aerr := filepath.Abs(exePath); aerr == nil {
		exePath = abs
	}
	baseDir := filepath.Dir(exePath)

	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kprobe_service_info",
		Help: "宿主服务身份信息(可执行文件/配置/日志路径), 恒为 1.",
	}, []string{"exe_path", "config_path", "log_path"})

	paths := logPaths
	if len(paths) == 0 {
		paths = []string{""}
	}
	for _, lp := range paths {
		vec.WithLabelValues(exePath, resolveAbs(baseDir, configPath), resolveAbs(baseDir, lp)).Set(1)
	}
	return vec
}

// resolveAbs 把路径换算为绝对路径: 已是绝对路径则仅做清理(消除 . .. 等),
// 相对路径以 baseDir(可执行文件所在目录)为基准拼接; 空串原样返回
func resolveAbs(baseDir, p string) string {
	if p == "" {
		return ""
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(baseDir, p)
	}
	return filepath.Clean(p)
}
