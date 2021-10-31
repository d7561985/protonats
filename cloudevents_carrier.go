package protonats

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/extensions"
	"github.com/d7561985/tel"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	ErrTraceStateExtension = errors.New("cloudevents extension not contain key: " + extensions.TraceStateExtension)
)

// InjectDistributedTracingExtension injects the tracecontext from the context into the event as a DistributedTracingExtension
//
// If a DistributedTracingExtension is present in the provided event, its current value is replaced with the
// tracecontext obtained from the context.
func InjectDistributedTracingExtension(ctx context.Context, event *cloudevents.Event) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		tel.FromCtx(ctx).Warn("no span inside context")
		return
	}

	buf := bytes.NewBuffer(nil)

	if err := tel.FromCtxWithSpan(ctx).Tracer().Inject(span.Context(), opentracing.Binary, buf); err != nil {
		tel.FromCtx(ctx).Warn("inject", zap.Error(err))
		return
	}

	// encode for correct transfer
	data := base64.RawStdEncoding.EncodeToString(buf.Bytes())

	event.SetExtension(extensions.TraceStateExtension, data)
}

// ExtractDistributedTracingExtension extracts the tracecontext from the cloud event.
func ExtractDistributedTracingExtension(ctx context.Context, event *cloudevents.Event) (opentracing.SpanContext, error) {
	x, ok := event.Extensions()[extensions.TraceStateExtension]
	if !ok {
		return nil, ErrTraceStateExtension
	}

	v, ok := x.(string)
	if !ok {
		return nil, fmt.Errorf("carrier casting wrong type %T", x)
	}

	data, err := base64.RawStdEncoding.DecodeString(v)
	if err != nil {
		return nil, fmt.Errorf("base64 decode %w", err)
	}

	r := bytes.NewReader(data)
	span, err := tel.FromCtx(ctx).T().Extract(opentracing.Binary, r)
	if err != nil {
		return nil, fmt.Errorf("extract error: %w", err)
	}

	return span, nil
}
