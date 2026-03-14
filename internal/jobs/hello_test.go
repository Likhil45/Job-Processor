package jobs

import (
	"context"
	"testing"
)

func TestHello_Type(t *testing.T) {
	var h Hello
	if got := h.Type(); got != HelloType {
		t.Errorf("Type() = %q, want %q", got, HelloType)
	}
}

func TestHello_Handle(t *testing.T) {
	h := Hello{}
	ctx := context.Background()
	err := h.Handle(ctx, []byte("test"))
	if err != nil {
		t.Errorf("Handle() error = %v", err)
	}
}
