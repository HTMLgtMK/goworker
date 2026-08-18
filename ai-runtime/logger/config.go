package logger

// Config 是日志系统的配置，对应 config.yaml 的 log 段。
type Config struct {
	Level      string `yaml:"level"`        // debug/info/warn/error，非法值回落 info
	File       string `yaml:"file"`         // 日志文件路径；空 = 只输出 stderr
	MaxSizeMB  int    `yaml:"max_size_mb"`  // 单文件大小上限；<=0 用默认 10
	MaxAgeDays int    `yaml:"max_age_days"` // 保留天数；<=0 用默认 7
}

// Default 返回带默认值的日志配置。
func Default() Config {
	return Config{
		Level:      "info",
		MaxSizeMB:  10,
		MaxAgeDays: 7,
	}
}

// applyDefaults 把零值字段补齐为默认值。
func applyDefaults(cfg Config) Config {
	d := Default()
	if cfg.Level == "" {
		cfg.Level = d.Level
	}
	if cfg.MaxSizeMB <= 0 {
		cfg.MaxSizeMB = d.MaxSizeMB
	}
	if cfg.MaxAgeDays <= 0 {
		cfg.MaxAgeDays = d.MaxAgeDays
	}
	return cfg
}
