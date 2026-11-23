package mobile

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBuildAARUsesStableJavaPackage(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("build-aar.sh requires bash")
	}
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	fake := filepath.Join(dir, "gomobile")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GOMOBILE_ARGS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "build-aar.sh")
	cmd.Env = append(os.Environ(), "GOMOBILE="+fake, "GOMOBILE_ARGS="+argsFile, "GOWORKER_AAR="+filepath.Join(dir, "goworker.aar"), "GOWORKER_MOBILE_GO_TAGS=goworker_no_dispatcher")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build-aar.sh: %v\n%s", err, out)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "-javapkg=dev.tinguo.goworker\n") {
		t.Fatalf("gomobile arguments missing Java package prefix:\n%s", args)
	}
}
