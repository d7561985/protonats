package adapter

import (
	"context"
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/d7561985/tel"
	"github.com/d7561985/tel/kaf"
	"github.com/nats-io/nats.go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-client-go"

	"go.uber.org/zap"
)

// ExampleKHeader show how to use KHeader struct to inject already existent span
func ExampleKHeader() {
	var headers nats.Header = make(nats.Header)

	// input context should be root tel ctx
	span, ctx := opentracing.StartSpanFromContext(context.TODO(), "NATS PRODUCER")
	defer span.Finish()

	h := NewHeader(&headers)
	if err := span.Tracer().Inject(span.Context(), opentracing.TextMap, &h); err != nil {
		tel.FromCtx(ctx).Error("producer inject trace", zap.Error(err))
	}
}

// StartSpanFromConsumerKafka show how helper function StartSpanFromConsumerKafka create new span for kafka consumer
func ExampleStartSpanFromConsumerKafka() {
	var headers = make(nats.Header)

	msg := cloudevents.Event{}

	// input context should be root tel ctx
	span, ctx := kaf.StartSpanFromConsumerKafka(context.TODO(), "NATS CONSUMER", &kaf.Message{
		Topic:     msg.Type(),
		Key:       []byte(msg.ID()),
		Timestamp: msg.Time(),
		//Partition: 0,
		//Offset:    0,
		Header: NewHeader(&headers),
	})

	tel.FromCtx(ctx).Info("new tele instance which should provided")

	ext.SpanKind.Set(span, "consumer")
}

func TestNHeader(t *testing.T) {
	const (
		key = "key"
		val = "value"
	)

	var headers = make(nats.Header)

	trace, closer := jaeger.NewTracer("X", jaeger.NewConstSampler(true), jaeger.NewNullReporter())
	defer closer.Close()

	span := trace.StartSpan("TEST")
	defer span.Finish()

	headerLen := 0
	headers[key] = []string{val}

	headerLen++

	err := trace.Inject(span.Context(), opentracing.TextMap, NewHeader(&headers))
	headerLen++

	assert.NoError(t, err)
	assert.NotEmpty(t, headers)
	assert.Len(t, headers, headerLen)

	// trace should add to the end, so first index already existed
	assert.Equal(t, headers[key], []string{val})

	fmt.Println(headers)
}
