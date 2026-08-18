package config

import "testing"

func TestParseContextWindow(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"32768", 32768, true},
		{"32k", 32768, true}, // 1024 进制，命中主流模型窗口
		{"32K", 32768, true},
		{"128k", 131072, true},
		{"1.5m", 1572864, true},
		{"4M", 4194304, true},
		{" 64k ", 65536, true}, // 容忍首尾空格
		{"", 0, false},
		{"abc", 0, false},
		{"k", 0, false},
		{"0", 0, false},
		{"-1k", 0, false},
		{"12x", 0, false},
		{"1.5.5k", 0, false},
	}
	for _, c := range cases {
		got, err := ParseContextWindow(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("ParseContextWindow(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseContextWindow(%q) = %d, nil; want error", c.in, got)
		}
	}
}

func TestParseCompressAt(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"0.8", 0.8, true},
		{"0", 0, true},
		{"1", 1, true},
		{"1.5", 0, false},
		{"-0.1", 0, false},
		{"NaN", 0, false},
		{"abc", 0, false},
	}
	for _, c := range cases {
		got, err := ParseCompressAt(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("ParseCompressAt(%q) = %v, %v; want %v, nil", c.in, got, err, c.want)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseCompressAt(%q) = %v, nil; want error", c.in, got)
		}
	}
}

func TestParseCompactKeepAndMaxIterations(t *testing.T) {
	for name, fn := range map[string]func(string) (int, error){
		"compact_keep":   ParseCompactKeep,
		"max_iterations": ParseMaxIterations,
	} {
		if _, err := fn("10"); err != nil {
			t.Errorf("%s: 10 -> %v", name, err)
		}
		if _, err := fn("0"); err == nil {
			t.Errorf("%s: 0 should error", name)
		}
		if _, err := fn("-3"); err == nil {
			t.Errorf("%s: -3 should error", name)
		}
		if _, err := fn("abc"); err == nil {
			t.Errorf("%s: abc should error", name)
		}
	}
}
