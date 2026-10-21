package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextCrossesRecordHeaders(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "publish")
	defer span.End()

	rec := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("stale")}, {Key: "other", Value: []byte("x")}}}
	prop := propagation.TraceContext{}
	prop.Inject(ctx, HeaderCarrier{Record: rec})

	if len(rec.Headers) != 2 {
		t.Fatalf("traceparent should replace the stale header, got %d headers", len(rec.Headers))
	}
	got := trace.SpanContextFromContext(prop.Extract(context.Background(), HeaderCarrier{Record: rec}))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("extracted %v, want child of %v", got, span.SpanContext())
	}
}
