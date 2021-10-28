package protonats

import (
	"bytes"
	"context"
	"fmt"

	cn "github.com/cloudevents/sdk-go/protocol/nats/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/d7561985/protonats/adapter"
	"github.com/d7561985/tel"
	"github.com/nats-io/nats.go"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

type Sender struct {
	*cn.Sender
}

// NewSender creates a new protocol.Sender responsible for opening and closing the STAN connection
func NewSender(url, subject string, natsOpts []nats.Option, opts ...cn.SenderOption) (protocol.SendCloser, error) {
	s, err := cn.NewSender(url, subject, natsOpts, opts...)
	if err != nil {
		return nil, err
	}

	return &Sender{Sender: s}, nil
}

// NewSenderFromConn creates a new protocol.Sender which leaves responsibility for opening and closing the STAN
// connection to the caller
func NewSenderFromConn(conn *nats.Conn, subject string, opts ...cn.SenderOption) (*Sender, error) {
	s, err := cn.NewSenderFromConn(conn, subject, opts...)
	if err != nil {
		return nil, err
	}

	return &Sender{Sender: s}, nil
}

func (s *Sender) Send(ctx context.Context, in binding.Message, transformers ...binding.Transformer) (err error) {
	defer func() {
		if err2 := in.Finish(err); err2 != nil {
			if err == nil {
				err = err2
			} else {
				err = fmt.Errorf("failed to call in.Finish() when error already occurred: %s: %w", err2.Error(), err)
			}
		}
	}()

	writer := new(bytes.Buffer)
	if err = cn.WriteMsg(ctx, in, writer, transformers...); err != nil {
		return err
	}

	// allow get topic
	subject := s.Subject
	if topic := cecontext.TopicFrom(ctx); topic != "" {
		subject = topic
	}

	h := make(nats.Header)

	span, _ := tel.StartSpanFromContext(ctx, "-> NATS://"+subject)
	if err = span.T().T().Inject(span.Context(), opentracing.TextMap, adapter.NewHeader(&h)); err != nil {
		return errors.WithStack(err)
	}

	return s.Conn.PublishMsg(&nats.Msg{
		Subject: subject,
		Header:  h,
		Data:    writer.Bytes(),
	})
}
