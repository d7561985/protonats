/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package protonats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

type Dryer interface {
	Drain() error
}

// The Subscriber interface allows us to configure how the subscription is created
type Subscriber interface {
	Subscribe(conn *nats.Conn, subject string, cn chan *nats.Msg) (Dryer, error)
}

// RegularSubscriber creates regular subscriptions
type RegularSubscriber struct {
}

// Subscribe implements Subscriber.Subscribe
func (s *RegularSubscriber) Subscribe(conn *nats.Conn, subject string, cn chan *nats.Msg) (Dryer, error) {
	return conn.ChanSubscribe(subject, cn)
}

var _ Subscriber = (*RegularSubscriber)(nil)

// QueueSubscriber creates queue subscriptions
type QueueSubscriber struct {
	Queue string
}

// Subscribe implements Subscriber.Subscribe
func (s *QueueSubscriber) Subscribe(conn *nats.Conn, subject string, cn chan *nats.Msg) (Dryer, error) {
	return conn.ChanQueueSubscribe(subject, s.Queue, cn)
}

var _ Subscriber = (*QueueSubscriber)(nil)

type SubjectQueuePool struct {
	Queue    string
	Subjects []string
}

func (s *SubjectQueuePool) Subscribe(conn *nats.Conn, _ string, cn chan *nats.Msg) (Dryer, error) {
	dryList := make(DrainList, 0, len(s.Subjects))

	for _, subject := range s.Subjects {
		d, err := conn.ChanQueueSubscribe(subject, s.Queue, cn)
		if err != nil {
			return nil, fmt.Errorf("subject %q subscribe error %v (drain result: %v)", subject, err, dryList.Drain())
		}

		dryList = append(dryList, d)
	}

	return dryList, nil
}

// DrainList simple cleaner which on drain error note call left drains
type DrainList []Dryer

func (d DrainList) Drain() error {
	for _, dryer := range d {
		if err := dryer.Drain(); err != nil {
			return err
		}
	}

	return nil
}

var _ Dryer = (DrainList)(nil)
