package main

import (
	"context"
	"errors"
	"testing"
)

func TestRetryJobUpdateDBOperationRetriesDeadlineOnce(t *testing.T) {
	attempts := 0
	err := retryJobUpdateDBOperation(context.Background(), "run", "lease renewal", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Fatalf("operation error = %v, want success after retry", err)
	}
	if attempts != 2 {
		t.Fatalf("operation attempts = %d, want 2", attempts)
	}
}

func TestRetryJobUpdateDBOperationStopsAfterSecondDeadline(t *testing.T) {
	attempts := 0
	err := retryJobUpdateDBOperation(context.Background(), "run", "lease renewal", func(context.Context) error {
		attempts++
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("operation error = %v, want context deadline exceeded", err)
	}
	if attempts != 2 {
		t.Fatalf("operation attempts = %d, want 2", attempts)
	}
}

func TestRetryJobUpdateDBOperationDoesNotRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := retryJobUpdateDBOperation(ctx, "run", "lease renewal", func(ctx context.Context) error {
		attempts++
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("operation error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("operation attempts = %d, want 1", attempts)
	}
}
