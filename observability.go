package protonats

import (
	"context"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/d7561985/tel"
	"github.com/d7561985/tel/monitoring/metrics"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

type TeleObservability struct {
	*tel.Telemetry
	Metrics metrics.MetricsReader
}

func (t *TeleObservability) RecordSendingEvent(_ctx context.Context, event event.Event) (context.Context, func(errOrResult error)) {
	span, ctx := tel.StartSpanFromContext(_ctx, event.String())
	start := time.Now()

	cb := func(err error) {
		defer span.Finish()

		t.Metrics.AddReaderTopicHandlingTime(event.Type(), time.Since(start))
		span.PutFields(zap.Duration("duration", time.Since(start)))

		if err != nil {
			t.Metrics.AddReaderTopicFatalError(event.Type(), 1)
			t.Metrics.AddReaderTopicProcessError(event.Type())
			t.Metrics.AddReaderTopicErrorEvents(event.Type(), 1)

			span.PutFields(zap.Error(err))
			return
		}

		t.Metrics.AddReaderTopicDecodeEvents(event.Type(), 1)
	}

	t.Metrics.AddReaderTopicReadEvents(event.Type(), 1)

	tel.FromCtx(ctx).PutFields(
		zap.String("type", event.Type()),
		zap.String("info", event.String()),
	)

	return ctx, cb
}

func (t *TeleObservability) InboundContextDecorators() []func(context.Context, binding.Message) context.Context {
	return []func(context.Context, binding.Message) context.Context{}
}

func (t TeleObservability) RecordReceivedMalformedEvent(ctx context.Context, err error) {
	panic("implement me")
}

func (t TeleObservability) RecordCallingInvoker(_ctx context.Context, e *event.Event) (context.Context, func(errOrResult error)) {
	opt := make([]opentracing.StartSpanOption, 0, 4)
	opt = append(opt, opentracing.Tags{
		"cloud.event.type": e.Type(),
		"key":              e.ID(),
	})

	if ref := _ctx.Value(opentracing.SpanReference{}); ref != nil {
		if v, ok := ref.(opentracing.SpanReference); ok {
			opt = append(opt, v)
		}
	}

	s := t.T().StartSpan(e.Type(), opt...)
	ext.Component.Set(s, "cloud.events.protocol.nats.observability")
	ext.SpanKindConsumer.Set(s)

	ctx := opentracing.ContextWithSpan(t.Ctx(), s)
	tel.UpdateTraceFields(ctx)

	return ctx, func(errOrResult error) {}
}

func (t TeleObservability) RecordRequestEvent(ctx context.Context, e event.Event) (context.Context, func(error, *event.Event)) {
	return ctx, func(errOrResult error, e *event.Event) {}
}
