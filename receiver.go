/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package protonats

import (
	"context"
	"io"
	"sync"

	cn "github.com/cloudevents/sdk-go/protocol/nats/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/nats-io/nats.go"
)

type NatsReceiver interface {
	protocol.Receiver
}

var _ protocol.Receiver = (*Receiver)(nil)

type Receiver struct {
	incoming <-chan *nats.Msg
}

func NewReceiver(ch <-chan *nats.Msg) NatsReceiver {
	return &Receiver{
		incoming: ch,
	}
}

func (r *Receiver) Receive(ctx context.Context) (binding.Message, error) {
	select {
	case in, ok := <-r.incoming:
		if !ok {
			return nil, io.EOF
		}

		return cn.NewMessage(in), nil
	case <-ctx.Done():
		return nil, io.EOF
	}
}

type Consumer struct {
	NatsReceiver

	// receivers chan
	ch chan *nats.Msg

	Conn       *nats.Conn
	Subject    string
	Subscriber Subscriber

	subMtx        sync.Mutex
	internalClose chan struct{}
	connOwned     bool
}

func NewConsumer(url, subject string, natsOpts []nats.Option, opts ...ConsumerOption) (*Consumer, error) {
	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	c, err := NewConsumerFromConn(conn, subject, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	c.connOwned = true

	return c, err
}

func NewConsumerFromConn(conn *nats.Conn, subject string, opts ...ConsumerOption) (*Consumer, error) {
	ch := make(chan *nats.Msg)

	c := &Consumer{
		ch:            ch,
		NatsReceiver:  NewReceiver(ch),
		Conn:          conn,
		Subject:       subject,
		Subscriber:    &RegularSubscriber{},
		internalClose: make(chan struct{}, 1),
	}

	err := c.applyOptions(opts...)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Consumer) OpenInbound(ctx context.Context) error {
	c.subMtx.Lock()
	defer c.subMtx.Unlock()

	// Subscribe
	sub, err := c.Subscriber.Subscribe(c.Conn, c.Subject, c.ch)
	if err != nil {
		return err
	}

	// Wait until external or internal context done
	select {
	case <-ctx.Done():
	case <-c.internalClose:
	}

	// Finish to consume messages in the queue and close the subscription
	return sub.Drain()
}

func (c *Consumer) Close(_ context.Context) error {
	// Before closing, let's be sure OpenInbound completes
	// We send a signal to close and then we lock on subMtx in order
	// to wait OpenInbound to finish draining the queue
	c.internalClose <- struct{}{}
	c.subMtx.Lock()
	defer c.subMtx.Unlock()

	if c.connOwned {
		c.Conn.Close()
	}

	close(c.internalClose)
	close(c.ch)

	return nil
}

type ConsumerOption func(*Consumer) error

func (c *Consumer) applyOptions(opts ...ConsumerOption) error {
	for _, fn := range opts {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}

var _ protocol.Opener = (*Consumer)(nil)
var _ protocol.Receiver = (*Consumer)(nil)
var _ protocol.Closer = (*Consumer)(nil)
