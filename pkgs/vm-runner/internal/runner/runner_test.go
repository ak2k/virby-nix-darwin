package runner

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"vm-runner/internal/config"
	"vm-runner/internal/signal_manager"
)

// fakeVM follows the real VMProcess: Start publishes the driver process only after
// killing orphans and exec'ing it, IsRunning and the VM's API then report it running,
// and Start returns once the VM boots.
type fakeVM struct {
	callers int32
	entered atomic.Int32
	ready   atomic.Bool
	running atomic.Bool
	starts  atomic.Int32
}

func (f *fakeVM) IPAddress() string    { return "" }
func (f *fakeVM) IsRunning() bool      { return f.running.Load() }
func (f *fakeVM) PauseOrStop() error   { return nil }
func (f *fakeVM) Stop(_ time.Duration) {}

func (f *fakeVM) ResumeOrStart() error {
	if f.running.Load() {
		return nil
	}
	return f.Start()
}

func (f *fakeVM) Start() error {
	f.starts.Add(1)
	time.Sleep(10 * time.Millisecond)
	f.running.Store(true)

	// Boot until every caller has had time to reach the readiness check.
	for f.entered.Load() < f.callers {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	f.ready.Store(true)
	return nil
}

// An on-demand VM receives a burst of SSH connections, one per build job, while it is stopped.
func TestConcurrentConnectionsStartVMOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		onDemand bool
	}{
		{"on-demand", true},
		{"always-on", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const callers = 8

			vm := &fakeVM{callers: callers}
			r := &Runner{
				config:        &config.VMConfig{OnDemand: tc.onDemand},
				signalManager: signal_manager.NewSignalManager(),
				vmProcess:     vm,
			}

			release := make(chan struct{})
			var wg sync.WaitGroup
			for range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-release
					vm.entered.Add(1)
					if err := r.ensureVMReady(); err != nil {
						t.Errorf("ensureVMReady: %v", err)
					}
					if !vm.ready.Load() {
						t.Error("ensureVMReady returned before the VM finished booting")
					}
				}()
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			close(release)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("ensureVMReady callers did not return")
			}

			if n := vm.starts.Load(); n != 1 {
				t.Errorf("VM started %d times for %d concurrent connections, want 1", n, callers)
			}
		})
	}
}
