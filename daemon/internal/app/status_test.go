package app

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tinguo/goworker/daemon/internal/app/config"
)

// newStatusTestApp 构造最小可查询装配：仅 Mode/startedAt/Config 参与 StatusInfo。
func newStatusTestApp(t *testing.T) *Application {
	t.Helper()
	return &Application{
		Mode:      "daemon",
		startedAt: time.Now(),
		Config: &config.Config{
			Frontend: config.FrontendConfig{ACP: config.ACPFrontendConfig{Enabled: true}},
		},
	}
}

func TestStatusInfoSocketDerivation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORKER_CONFIG_DIR", dir)

	info := newStatusTestApp(t).StatusInfo()

	if got, want := info.ACPSocket, filepath.Join(dir, "frontend", "acp.sock"); got != want {
		t.Errorf("ACPSocket = %q, want derived default %q", got, want)
	}
	if got, want := info.DispatchSocket, filepath.Join(dir, "dispatch", "acp.sock"); got != want {
		t.Errorf("DispatchSocket = %q, want %q", got, want)
	}
	if got, want := info.StatusSocket, filepath.Join(dir, "status.sock"); got != want {
		t.Errorf("StatusSocket = %q, want %q", got, want)
	}
	if got, want := info.ConfigPath, filepath.Join(dir, "config.yaml"); got != want {
		t.Errorf("ConfigPath = %q, want %q", got, want)
	}
	if info.Mode != "daemon" {
		t.Errorf("Mode = %q, want %q", info.Mode, "daemon")
	}
}

func TestStatusInfoOmitsDisabledACP(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())

	a := newStatusTestApp(t)
	a.Config.Frontend.ACP.Enabled = false

	if got := a.StatusInfo().ACPSocket; got != "" {
		t.Errorf("ACPSocket = %q, want empty when frontend.acp disabled", got)
	}
}

func TestStatusInfoJSONRoundTrip(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())

	data, err := json.Marshal(newStatusTestApp(t).StatusInfo())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back StatusInfo
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.PID != os.Getpid() {
		t.Errorf("PID = %d, want %d", back.PID, os.Getpid())
	}
	if back.Version == "" {
		t.Error("Version is empty in JSON snapshot")
	}
	if _, err := time.Parse(time.RFC3339, back.StartedAt); err != nil {
		t.Errorf("StartedAt %q not RFC3339: %v", back.StartedAt, err)
	}
}

func TestStatusServiceSnapshotOverSocket(t *testing.T) {
	// Unix socket 路径上限 104 字节（macOS sun_path），t.TempDir() 的
	// var/folders 长路径 bind 会报 invalid argument，换成短路径基座。
	dir, err := os.MkdirTemp("/tmp", "gw-status-test")
	if err != nil {
		t.Fatalf("create short temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	t.Setenv("GOWORKER_CONFIG_DIR", dir)

	a := newStatusTestApp(t)
	svc := &statusService{app: a}
	if err := svc.Start(); err != nil {
		t.Fatalf("start status service: %v", err)
	}

	conn, err := net.DialTimeout("unix", StatusSocketPath(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial status socket: %v", err)
	}
	readDone := make(chan []byte, 1)
	go func() {
		defer conn.Close()
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		readDone <- buf[:n]
	}()

	select {
	case raw := <-readDone:
		var info StatusInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			t.Fatalf("snapshot %q: %v", raw, err)
		}
		if info.Mode != "daemon" || info.PID != os.Getpid() {
			t.Errorf("snapshot mismatch: mode=%q pid=%d", info.Mode, info.PID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for status snapshot")
	}

	if err := svc.Stop(); err != nil {
		t.Fatalf("stop status service: %v", err)
	}
	// Stop 应清掉 socket 文件，`goworker status` 才能把"未运行"和"残留"区分开。
	if _, err := os.Stat(StatusSocketPath()); !os.IsNotExist(err) {
		t.Errorf("status socket still present after Stop: %v", err)
	}
}
