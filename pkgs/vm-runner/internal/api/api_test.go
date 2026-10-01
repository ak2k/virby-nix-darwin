package api

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The VMProcess closes its client whenever a VM stops and keeps using it for the next VM.
func TestCloseTwiceThenCallReturnsError(t *testing.T) {
	// Nothing listens on port 0, so the call fails without needing the network.
	client := NewAPIClient(0, nil)

	client.Close()
	client.Close()

	if _, err := client.Get("/vm/state"); err == nil {
		t.Fatal("expected an error from a port nothing listens on")
	}
}

func TestClientServesRequestsAfterClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback: %v", err)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"state":"VirtualMachineStateRunning"}`))
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	client := NewAPIClient(listener.Addr().(*net.TCPAddr).Port, nil)
	client.Close()

	state, err := client.Get("/vm/state")
	if err != nil {
		t.Fatalf("request after Close failed: %v", err)
	}
	if state["state"] != string(VMStateRunning) {
		t.Fatalf("unexpected state: %v", state["state"])
	}
}
