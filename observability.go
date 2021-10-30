package protonats

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/d7561985/tel"
	"github.com/d7561985/tel/monitoring/metrics"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

// TeleObservability implement cloudevents client.ObservabilityService with OpenTracing propagation
// This flow idempotent and not tight coupled to NATS
// and can easily treat any opentracing flow which provides correct context values
//
// Producer component handled in RecordSendingEvent and has specific context requirements
type TeleObservability struct {
	*tel.Telemetry
	Metrics metrics.MetricsReader
}

// RecordSendingEvent producer interceptor required context argument to be polluted with tel context
// creates new tracing brunch from provided inside context OpenTracing or tel data
func (t *TeleObservability) RecordSendingEvent(_ctx context.Context, e event.Event) (context.Context, func(errOrResult error)) {
	span, ctx := tel.StartSpanFromContext(_ctx, fmt.Sprintf("SEND EVENT: %s", e.Type()))
	start := time.Now()

	ext.Component.Set(span, "cloud.events.protocol.nats.observability")
	ext.SpanKindProducer.Set(span)

	cb := func(err error) {
		defer span.Finish()

		t.Metrics.AddReaderTopicHandlingTime(e.Type(), time.Since(start))
		span.PutFields(zap.Duration("duration", time.Since(start)))

		if err != nil {
			t.Metrics.AddReaderTopicFatalError(e.Type(), 1)
			t.Metrics.AddReaderTopicProcessError(e.Type())
			t.Metrics.AddReaderTopicErrorEvents(e.Type(), 1)

			span.PutFields(zap.Error(err))
			return
		}

		t.Metrics.AddReaderTopicDecodeEvents(e.Type(), 1)
	}

	t.Metrics.AddReaderTopicReadEvents(e.Type(), 1)

	span.PutFields(
		zap.String("cloud.event.type", e.Type()),
		zap.String("key", e.ID()),
		zap.String("info", e.String()),
	)

	return ctx, cb
}

func (t *TeleObservability) InboundContextDecorators() []func(context.Context, binding.Message) context.Context {
	return []func(context.Context, binding.Message) context.Context{}
}

func (t TeleObservability) RecordReceivedMalformedEvent(_ context.Context, _ error) {
	panic("implement me")
}

// RecordCallingInvoker consumer middleware
// expect special data inside containing opentracing.SpanReference which receiver should put inside
//˚
// consumer represent invoker model for opentracing,
//from tracing objectives this mean that it begin new span either and that span should be return and used by others
func (t *TeleObservability) RecordCallingInvoker(_ctx context.Context, e *event.Event) (context.Context, func(errOrResult error)) {
	opt := make([]opentracing.StartSpanOption, 0, 4)
	opt = append(opt, opentracing.Tags{
		"cloud.event.type": e.Type(),
		"key":              e.ID(),
		"event info":       e.String(),
	})

	if ref := _ctx.Value(opentracing.SpanReference{}); ref != nil {
		if v, ok := ref.(opentracing.SpanReference); ok {
			opt = append(opt, v)
		}
	}

	s := t.T().StartSpan(fmt.Sprintf("GET EVENT: %s", e.Type()), opt...)
	ext.Component.Set(s, "cloud.events.protocol.nats.observability")
	ext.SpanKindConsumer.Set(s)

	ctx := opentracing.ContextWithSpan(t.Copy().Ctx(), s)
	tel.UpdateTraceFields(ctx)

	return ctx, func(errOrResult error) {}
}

func (t TeleObservability) RecordRequestEvent(ctx context.Context, _ event.Event) (context.Context, func(error, *event.Event)) {
	return ctx, func(errOrResult error, e *event.Event) {}
}
