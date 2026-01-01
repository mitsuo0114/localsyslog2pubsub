package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

type mockPublisher struct {
	mu       sync.Mutex
	requests []*pubsubpb.PublishRequest
	err      error
}

func (m *mockPublisher) Publish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, req)
	if m.err != nil {
		return nil, m.err
	}
	return &pubsubpb.PublishResponse{}, nil
}

func TestPublishLine(t *testing.T) {
	mock := &mockPublisher{}
	ctx := context.Background()
	data := []byte("hello")

	if err := publishLine(ctx, mock, "projects/test/topics/topic", data); err != nil {
		t.Fatalf("publishLine returned error: %v", err)
	}

	if got := len(mock.requests); got != 1 {
		t.Fatalf("expected 1 request, got %d", got)
	}

	req := mock.requests[0]
	if req.Topic != "projects/test/topics/topic" {
		t.Errorf("unexpected topic: %s", req.Topic)
	}
	if len(req.Messages) != 1 || string(req.Messages[0].Data) != "hello" {
		t.Fatalf("unexpected messages: %+v", req.Messages)
	}
}

func TestHandleConnPublishesAndLogs(t *testing.T) {
	mock := &mockPublisher{}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var outBuf, errBuf bytes.Buffer
	outLogger := log.New(&outBuf, "", 0)
	errLogger := log.New(&errBuf, "", 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		handleConn(ctx, serverConn, mock, "projects/test/topics/topic", outLogger, errLogger)
	}()

	lines := []string{"first line", "second line"}
	for _, line := range lines {
		if _, err := clientConn.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}
	_ = clientConn.Close()

	wg.Wait()

	if got := len(mock.requests); got != len(lines) {
		t.Fatalf("expected %d publish calls, got %d", len(lines), got)
	}

	for i, req := range mock.requests {
		if string(req.Messages[0].Data) != lines[i] {
			t.Errorf("message %d data = %q, want %q", i, req.Messages[0].Data, lines[i])
		}
	}

	for _, line := range lines {
		if !strings.Contains(outBuf.String(), line) {
			t.Errorf("stdout log missing line %q", line)
		}
	}

	if errBuf.Len() != 0 {
		t.Errorf("unexpected error log output: %s", errBuf.String())
	}
}
