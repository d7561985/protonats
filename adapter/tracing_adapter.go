package adapter

import (
	"github.com/d7561985/tel/kaf"
	"github.com/nats-io/nats.go"
	"github.com/uber/jaeger-client-go"
)

// KHeader implements TextMap opentracing building format
// note: values are stored just via sting casting is it ok?
// Example:
// 		spanCtx, err := trace.Extract(opentracing.TextMap, KHeader(e.Headers))
//				if err == nil {
//					opt = append(opt, opentracing.ChildOf(spanCtx))
//				}
//
//	var headers []kafka.Header
//	h := KHeader(headers)
//	err := trace.Inject(span.Context(), opentracing.TextMap, &h)
type kHeader struct {
	h *nats.Header
}

func NewHeader(in *nats.Header) kaf.KHeader {
	return &kHeader{h: in}
}

func (h *kHeader) Set(key, val string) {
	(*h.h)[key] = []string{val}
}

func (h kHeader) ForeachKey(handler func(key string, val string) error) error {
	for k, ll := range *h.h {
		for _, v := range ll {
			if err := handler(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *kHeader) GetTraceValue() []byte {
	for key, v := range *h.h {
		if key != jaeger.TraceContextHeaderName {
			continue
		}

		return []byte(v[0])
	}

	return nil
}
