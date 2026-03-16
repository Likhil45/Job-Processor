package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
)

const EmailType = "email"

// EmailPayload is the JSON payload for email jobs.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// Email sends email via SMTP (e.g. MailHog in dev).
type Email struct {
	SMTPAddr string // e.g. "localhost:1025"
	From     string // e.g. "noreply@localhost"
}

// NewEmailHandler returns an email handler. SMTPAddr and From can be empty for mock/no-op.
func NewEmailHandler(smtpAddr, from string) *Email {
	if from == "" {
		from = "noreply@localhost"
	}
	return &Email{SMTPAddr: smtpAddr, From: from}
}

func (e *Email) Type() string { return EmailType }

// ValidateEmailPayload checks payload without sending. Used when writing to outbox.
func ValidateEmailPayload(payload []byte) error {
	var p EmailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("email payload: %w", err)
	}
	if strings.TrimSpace(p.To) == "" {
		return fmt.Errorf("email: missing to")
	}
	return nil
}

func (e *Email) Handle(ctx context.Context, payload []byte) error {
	if err := ValidateEmailPayload(payload); err != nil {
		return err
	}
	var p EmailPayload
	_ = json.Unmarshal(payload, &p)
	if e.SMTPAddr == "" {
		slog.Info("email job (no SMTP)", "to", p.To, "subject", p.Subject)
		return nil
	}
	msg := []byte(
		"To: " + p.To + "\r\n" +
			"Subject: " + p.Subject + "\r\n" +
			"\r\n" + p.Body + "\r\n")
	err := smtp.SendMail(e.SMTPAddr, nil, e.From, []string{p.To}, msg)
	if err != nil {
		return fmt.Errorf("smtp send: %w", err)
	}
	slog.Info("email sent", "to", p.To, "subject", p.Subject)
	return nil
}
