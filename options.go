package protonats

import (
	"errors"

	"github.com/cloudevents/sdk-go/protocol/nats/v2"
)

var ErrEmptySubject = errors.New("empty subject list")

// WithQueueSubscriber configures the Consumer to join a queue group when subscribing
func WithQueueSubscriber(queue string) ConsumerOption {
	return func(c *Consumer) error {
		if queue == "" {
			return nats.ErrInvalidQueueName
		}
		c.Subscriber = &QueueSubscriber{Queue: queue}
		return nil
	}
}

// WithQueuePoolSubscriber create subject list pool for specific queue
func WithQueuePoolSubscriber(queue string, subject ...string) ConsumerOption {
	return func(c *Consumer) error {
		if queue == "" {
			return nats.ErrInvalidQueueName
		}

		if len(subject) == 0 {
			return ErrEmptySubject
		}

		c.Subscriber = &SubjectQueuePool{Queue: queue, Subjects: subject}
		return nil
	}
}

type ObservabilityOption func(*TeleObservability)

// WithSpanAttributesGetter appends the returned attributes from the function to the span.
func WithSpanAttributesGetter(attrGetter SpanAttrGetter) ObservabilityOption {
	return func(os *TeleObservability) {
		if attrGetter != nil {
			os.spanAttributesGetter = attrGetter
		}
	}
}

// WithSpanNameFormatter replaces the default span name with the string returned from the function
func WithSpanNameFormatter(nameFormatter SpanNameFormatter) ObservabilityOption {
	return func(os *TeleObservability) {
		if nameFormatter != nil {
			os.spanNameFormatter = nameFormatter
		}
	}
}
