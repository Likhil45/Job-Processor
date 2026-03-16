package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitTracer initializes the global tracer provider and W3C propagator.
// getEnv(key, default) is used for OTEL_EXPORTER_OTLP_ENDPOINT (if set, use OTLP HTTP; else stdout).
// If debugLogPath is non-empty, NDJSON debug lines are appended there (hypotheses A,B,D,E).
// Returns a shutdown function to flush and stop the provider.
func InitTracer(serviceName string, getEnv func(string, string) string, debugLogPath string) (shutdown func(), err error) {
	ctx := context.Background()
	endpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	// #region agent log
	if debugLogPath != "" {
		debugWrite(debugLogPath, "InitTracer started", map[string]interface{}{"hypothesisId": "B,E", "endpoint": endpoint, "serviceName": serviceName})
	}
	// #endregion
	var exp sdktrace.SpanExporter
	if endpoint != "" {
		exp, err = newOTLPExporter(ctx, endpoint)
		// #region agent log
		if debugLogPath != "" {
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			debugWrite(debugLogPath, "OTLP exporter create", map[string]interface{}{"hypothesisId": "A,E", "err": errStr, "endpoint": endpoint})
		}
		// #endregion
		if err != nil {
			return nil, err
		}
		slog.Info("tracing: using OTLP exporter", "endpoint", endpoint)
	} else {
		exp, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, err
		}
		slog.Info("tracing: using stdout exporter")
	}
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
		resource.WithSchemaURL(semconv.SchemaURL),
	)
	// #region agent log
	if debugLogPath != "" {
		debugWrite(debugLogPath, "resource created", map[string]interface{}{"hypothesisId": "D", "serviceName": serviceName, "resource_err": err != nil})
	}
	// #endregion
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}
	// #region agent log
	if debugLogPath != "" {
		debugWrite(debugLogPath, "InitTracer done", map[string]interface{}{"hypothesisId": "A", "err": nil, "tracing_enabled": true})
	}
	// #endregion
	return shutdown, nil
}

func debugWrite(path, message string, data map[string]interface{}) {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["sessionId"] = "3a3d06"
	data["timestamp"] = time.Now().UnixMilli()
	data["location"] = "internal/telemetry/tracing.go"
	data["message"] = message
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_ = json.NewEncoder(f).Encode(data)
}

func newOTLPExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	endpoint = strings.TrimSuffix(endpoint, "/")
	// Strip scheme; SDK expects host:port and adds /v1/traces by default.
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}
	// Remove path if present so WithEndpoint gets host:port only.
	if idx := strings.Index(endpoint, "/"); idx > 0 {
		endpoint = endpoint[:idx]
	}
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	}
	return otlptracehttp.New(ctx, opts...)
}
