package jobs

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const ReportType = "report"

// ReportPayload is the JSON payload for report generation jobs.
// Rows is a list of string slices (each slice is a row; first row can be headers).
type ReportPayload struct {
	Headers []string     `json:"headers"`
	Rows    [][]string   `json:"rows"`
	OutPath string       `json:"out_path"`
}

// Report generates CSV reports from payload data.
type Report struct {
	OutputDir string
}

func (r *Report) Type() string { return ReportType }

func (r *Report) Handle(ctx context.Context, payload []byte) error {
	var p ReportPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("report payload: %w", err)
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if len(p.Headers) > 0 {
		if err := w.Write(p.Headers); err != nil {
			return fmt.Errorf("report headers: %w", err)
		}
	}
	for _, row := range p.Rows {
		if err := w.Write(row); err != nil {
			return fmt.Errorf("report row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	out := buf.Bytes()
	if p.OutPath == "" {
		slog.Info("report generated (no out_path)", "rows", len(p.Rows))
		return nil
	}
	outPath := p.OutPath
	if r.OutputDir != "" && !filepath.IsAbs(outPath) {
		outPath = filepath.Join(r.OutputDir, outPath)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("report mkdir: %w", err)
	}
	if err := os.WriteFile(outPath, out, 0644); err != nil {
		return fmt.Errorf("report write: %w", err)
	}
	slog.Info("report written", "path", outPath)
	return nil
}
