package jobs

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEmail_Type(t *testing.T) {
	e := NewEmailHandler("", "")
	if e.Type() != EmailType {
		t.Errorf("Type() = %q, want %q", e.Type(), EmailType)
	}
}

func TestEmail_Handle_NoSMTP(t *testing.T) {
	e := NewEmailHandler("", "")
	payload, _ := json.Marshal(EmailPayload{To: "test@example.com", Subject: "Hi", Body: "Hello"})
	err := e.Handle(context.Background(), payload)
	if err != nil {
		t.Errorf("Handle (no SMTP) = %v", err)
	}
}

func TestEmail_Handle_InvalidPayload(t *testing.T) {
	e := NewEmailHandler("", "")
	err := e.Handle(context.Background(), []byte("not json"))
	if err == nil {
		t.Error("Handle(invalid payload) expected error")
	}
}

func TestEmail_Handle_EmptyTo(t *testing.T) {
	e := NewEmailHandler("", "")
	payload, _ := json.Marshal(EmailPayload{To: "", Subject: "Hi", Body: "Hello"})
	err := e.Handle(context.Background(), payload)
	if err == nil {
		t.Error("Handle(empty to) expected error")
	}
}
