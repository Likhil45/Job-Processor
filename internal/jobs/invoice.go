package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"text/template"
)

const InvoiceType = "invoice"

// InvoicePayload is the JSON payload for invoice generation jobs.
type InvoicePayload struct {
	Template string            `json:"template"` // template body (e.g. "Invoice #{{.ID}}\nTotal: {{.Total}}")
	Data     map[string]any    `json:"data"`
	OutPath  string            `json:"out_path"` // optional output path
}

// Invoice renders a template with data and writes to file (or logs if no OutPath).
type Invoice struct {
	OutputDir string
}

func (i *Invoice) Type() string { return InvoiceType }

func (i *Invoice) Handle(ctx context.Context, payload []byte) error {
	var p InvoicePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invoice payload: %w", err)
	}
	// Demo: fail the first N attempts when Data has _demo_fail_until_attempt (int or float64).
	if p.Data != nil {
		if v, ok := p.Data["_demo_fail_until_attempt"]; ok {
			var until int
			switch n := v.(type) {
			case float64:
				until = int(n)
			case int:
				until = n
			}
			if attemptVal := ctx.Value(AttemptContextKey); attemptVal != nil {
				if a, ok := attemptVal.(int32); ok && int(a) < until {
					return fmt.Errorf("demo: failing attempt %d (succeed after %d retries)", a+1, until)
				}
			}
		}
	}
	if p.Template == "" {
		return fmt.Errorf("invoice: template required")
	}
	tpl, err := template.New("invoice").Parse(p.Template)
	if err != nil {
		return fmt.Errorf("invoice template: %w", err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, p.Data); err != nil {
		return fmt.Errorf("invoice execute: %w", err)
	}
	out := buf.Bytes()
	if p.OutPath == "" {
		slog.Info("invoice rendered (no out_path)", "size", len(out))
		return nil
	}
	outPath := p.OutPath
	if i.OutputDir != "" && !filepath.IsAbs(outPath) {
		outPath = filepath.Join(i.OutputDir, outPath)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("invoice mkdir: %w", err)
	}
	if err := os.WriteFile(outPath, out, 0644); err != nil {
		return fmt.Errorf("invoice write: %w", err)
	}
	slog.Info("invoice written", "path", outPath)
	return nil
}
