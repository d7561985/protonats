//go:build internal_tests
// +build internal_tests

package protonats_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/d7561985/protonats"
	"github.com/d7561985/tel"
	"github.com/d7561985/tel/monitoring/metrics"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
)

type Example struct {
	Sequence int    `json:"id"`
	Message  string `json:"message"`
}

const (
	TopicA = "topicA"
	TopicB = "topicB"
)

func TestA(t *testing.T) {
	p, err := protonats.NewProtocol("127.0.0.1:4222", "", TopicA,
		[]nats.Option{},
		protonats.WithConsumerOptions(
			protonats.WithQueuePoolSubscriber("api-service",
				TopicB, TopicA,
			),
		),
	)

	require.NoError(t, err)

	tr, closeer := jaeger.NewTracer("XXX", jaeger.NewConstSampler(false), jaeger.NewNullReporter())
	defer closeer.Close()

	opentracing.InitGlobalTracer(tr)

	tl := tel.NewNull()

	ctx := tl.Ctx()

	ce, err := cloudevents.NewClient(p,
		client.WithObservabilityService(
			protonats.NewTeleObservability(&tl, metrics.NewCollectorMetricsReader())),
	)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		err = ce.StartReceiver(ctx, func(ctx context.Context, event cloudevents.Event) error {
			fmt.Printf("Got Event Context: %+v\n", event.Context)

			data := &Example{}
			if err := event.DataAs(data); err != nil {
				fmt.Printf("Got Data Error: %s\n", err.Error())
			}
			fmt.Printf("Got Data: %+v\n", data)

			fmt.Println(event.Extensions())
			fmt.Printf("----------------------------\n")
			done <- struct{}{}
			return nil
		})

		assert.NoError(t, err)
	}()

	<-time.After(time.Second)

	e := cloudevents.NewEvent()
	e.SetID(uuid.New().String())
	e.SetType(TopicB)
	e.SetSource("api")
	e.SetDataSchema("my-schema-registry://xxx")
	_ = e.SetData(cloudevents.ApplicationJSON, &Example{
		Sequence: 1,
		Message:  "Hello World",
	})
	e.SetExtension("EXTENSION", "V2123")

	// inject topic
	ctx = cecontext.WithTopic(ctx, TopicB)

	err = ce.Send(ctx, e)
	assert.NoError(t, err)

	<-done
}
