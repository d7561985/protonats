package protonats

import (
	"context"
	"runtime"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/observability"
	"github.com/d7561985/tel"
	"github.com/d7561985/tel/monitoring/metrics"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

const componentName = "cloud.events.protocol.nats.observability"

type SpanNameFormatter func(cloudevents.Event) string
type SpanAttrGetter func(cloudevents.Event) opentracing.Tags

// TeleObservability implement cloudevents client.ObservabilityService with OpenTracing propagation
// This flow idempotent and not tight coupled to NATS
// and can easily treat any opentracing flow which provides correct context values
//
// Producer component handled in RecordSendingEvent and has specific context requirements
type TeleObservability struct {
	*tel.Telemetry

	Metrics metrics.MetricsReader

	spanAttributesGetter SpanAttrGetter
	spanNameFormatter    SpanNameFormatter
}

func NewTeleObservability(t *tel.Telemetry, m metrics.MetricsReader, opts ...ObservabilityOption) client.ObservabilityService {
	res := &TeleObservability{
		Telemetry:            t,
		Metrics:              m,
		spanAttributesGetter: nil,
		spanNameFormatter:    defaultSpanNameFormatter,
	}

	for _, fn := range opts {
		fn(res)
	}

	return res
}

// RecordSendingEvent producer interceptor required context argument to be polluted with tel context
// creates new tracing brunch from provided inside context OpenTracing or tel data
func (t *TeleObservability) RecordSendingEvent(_ctx context.Context, e event.Event) (context.Context, func(errOrResult error)) {
	attr := t.GetSpanAttributes(e, getFuncName())
	span, ctx := tel.StartSpanFromContext(_ctx, t.getSpanName(&e, "send"), attr)

	ext.Component.Set(span, componentName)
	ext.SpanKindProducer.Set(span)

	// inject tracing
	InjectDistributedTracingExtension(ctx, &e)

	cb := func(err error) {
		defer span.Finish()
	}

	return ctx, cb
}

// InboundContextDecorators returns a decorator function that allows enriching the context with the incoming parent trace.
// This method gets invoked automatically by passing the option 'WithObservabilityService' when creating the cloudevents client.
func (t *TeleObservability) InboundContextDecorators() []func(context.Context, binding.Message) context.Context {
	return []func(context.Context, binding.Message) context.Context{t.tracePropagatorContextDecorator}
}

// RecordCallingInvoker consumer middleware
// expect special data inside containing opentracing.SpanReference which receiver should put inside
//˚
// consumer represent invoker model for opentracing,
//from tracing objectives this mean that it begin new span either and that span should be return and used by others
func (t *TeleObservability) RecordCallingInvoker(_ctx context.Context, e *event.Event) (context.Context, func(errOrResult error)) {
	opt := make([]opentracing.StartSpanOption, 0, 2)
	opt = append(opt, t.GetSpanAttributes(*e, getFuncName()))

	spanCtx, err := ExtractDistributedTracingExtension(t.Ctx(), e)
	if err != nil {
		tel.FromCtx(_ctx).Error("extract distributed trace", zap.Error(err))
	} else {
		opt = append(opt, opentracing.ChildOf(spanCtx))
	}

	if ref := _ctx.Value(opentracing.SpanReference{}); ref != nil {
		if v, ok := ref.(opentracing.SpanReference); ok {
			opt = append(opt, v)
		}
	}

	tr, start := t.Copy(), time.Now()
	span, ctx := tr.StartSpan(t.getSpanName(e, "process"), opt...)

	ext.Component.Set(span, componentName)
	ext.SpanKindConsumer.Set(span)
	tel.UpdateTraceFields(ctx)

	cb := func(err error) {
		defer span.Finish()

		t.Metrics.AddReaderTopicHandlingTime(e.Type(), time.Since(start))
		span.PutFields(zap.String("duration", time.Since(start).String()))

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

	return ctx, cb
}

// RecordReceivedMalformedEvent if content is unpredictable
func (t *TeleObservability) RecordReceivedMalformedEvent(ctx context.Context, err error) {
	spanName := observability.ClientSpanName + ".malformed receive"
	span, _ := tel.FromCtx(ctx).StartSpan(spanName)
	defer span.Finish()

	ext.Component.Set(span, componentName)
	ext.SpanKindConsumer.Set(span)

	span.Error(spanName, zap.Error(err))
}

func (t TeleObservability) RecordRequestEvent(ctx context.Context, _ event.Event) (context.Context, func(error, *event.Event)) {
	return ctx, func(errOrResult error, e *event.Event) {}
}

// getSpanName Returns the name of the span.
//
// When no spanNameFormatter is present in OTelObservabilityService,
// the default name will be "cloudevents.client.<eventtype> prefix" e.g. cloudevents.client.get.customers send.
//
// The prefix is always added at the end of the span name. This follows the semantic conventions for
// messasing systems as defined in https://github.com/open-telemetry/opentelemetry-specification/blob/v1.6.1/specification/trace/semantic_conventions/messaging.md#operation-names
func (t TeleObservability) getSpanName(e *cloudevents.Event, suffix string) string {
	name := t.spanNameFormatter(*e)

	// make sure the span name ends with the suffix from the semantic conventions (receive, send, process)
	if !strings.HasSuffix(name, suffix) {
		return name + " " + suffix
	}

	return name
}

// Extracts the traceparent from the msg and enriches the context to enable propagation
func (t *TeleObservability) tracePropagatorContextDecorator(ctx context.Context, msg binding.Message) context.Context {
	return t.Copy().Ctx()
}

func getFuncName() string {
	pc := make([]uintptr, 1)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()

	// frame.Function should be github.com/cloudevents/sdk-go/observability/opentelemetry/v2/client.OTelObservabilityService.Func
	parts := strings.Split(frame.Function, ".")

	// we are interested in the function name
	if len(parts) != 4 {
		return ""
	}
	return parts[3]
}

// GetSpanAttributes returns the attributes that are always added to the spans
func (t *TeleObservability) GetSpanAttributes(e cloudevents.Event, method string) opentracing.Tags {
	attr := opentracing.Tags{
		"code.function":               method,
		observability.SpecversionAttr: e.SpecVersion(),
		observability.IdAttr:          e.ID(),
		observability.TypeAttr:        e.Type(),
		observability.SourceAttr:      e.Source(),
	}
	if sub := e.Subject(); sub != "" {
		attr[observability.SubjectAttr] = sub
	}
	if dct := e.DataContentType(); dct != "" {
		attr[observability.DatacontenttypeAttr] = dct
	}
	if t.spanAttributesGetter != nil {
		a := t.spanAttributesGetter(e)
		for k, v := range a {
			attr[k] = v
		}
	}

	return attr
}

var defaultSpanNameFormatter = func(e cloudevents.Event) string {
	return observability.ClientSpanName + "." + e.Context.GetType()
}
