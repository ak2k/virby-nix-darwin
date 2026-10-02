package vm_process

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
	"vm-runner/internal/config"
)

// A driver dies unexpectedly, and the next Start publishes a new driver before the old one's
// monitor takes the lock.
func TestMonitorReleasesOnlyItsOwnDriver(t *testing.T) {
	dir := t.TempDir()
	vp := &VMProcess{
		config:  &config.VMConfig{WorkingDirectory: dir},
		pidFile: filepath.Join(dir, "vm.pid"),
	}

	exited, held, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer exited.Close()

	first := exec.Command("sleep", "60")
	first.Stdout = held
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	held.Close()
	firstExit := make(chan struct{})
	vp.command = first
	vp.processExitCh = firstExit

	monitored := make(chan struct{})
	go func() {
		vp.monitorVM(first, nil, firstExit)
		close(monitored)
	}()

	second := exec.Command("sleep", "60")
	secondExit := make(chan struct{})

	vp.mu.Lock()
	first.Process.Kill()
	// EOF once the first driver has exited.
	io.ReadAll(exited)
	vp.command = second
	vp.processExitCh = secondExit
	vp.mu.Unlock()

	select {
	case <-monitored:
	case <-time.After(10 * time.Second):
		t.Fatal("first driver's monitor did not return")
	}

	select {
	case <-firstExit:
	default:
		t.Error("first driver's monitor did not close its exit channel")
	}

	select {
	case <-secondExit:
		t.Error("first driver's monitor closed the second driver's exit channel")
	default:
	}

	vp.mu.Lock()
	current := vp.command
	vp.mu.Unlock()
	if current != second {
		t.Error("first driver's monitor reset the second driver's state")
	}
}
