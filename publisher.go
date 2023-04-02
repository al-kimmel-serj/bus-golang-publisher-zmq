package bus_golang_publisher_zmq

import (
	"bytes"
	"context"
	"fmt"

	"github.com/al-kimmel-serj/bus-golang"
	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
)

const (
	TopicAndPayloadDelimiter        byte = 0
	TopicPrefixAndEventKeyDelimiter byte = 1
	TopicPrefixFormat                    = "%s:v%d" + string(TopicPrefixAndEventKeyDelimiter)
)

type Publisher[Payload proto.Message] struct {
	topicPrefix []byte
	unregister  func() error
	zmqContext  *zmq4.Context
	zmqSocket   *zmq4.Socket
}

func New[Payload proto.Message](
	host string,
	port int,
	eventName bus.EventName,
	eventVersion bus.EventVersion,
	publishersRegistry bus.PublishersRegistry,
) (*Publisher[Payload], error) {
	zmqContext, _ := zmq4.NewContext()
	zmqSocket, _ := zmqContext.NewSocket(zmq4.PUB)
	err := zmqSocket.Bind(fmt.Sprintf("tcp://*:%d", port))
	if err != nil {
		return nil, fmt.Errorf("zmq4.Socket.Bind error: %w", err)
	}

	topicPrefix := []byte(fmt.Sprintf(TopicPrefixFormat, eventName, eventVersion))

	unregister, err := publishersRegistry.Register(eventName, eventVersion, host, port)
	if err != nil {
		_ = zmqSocket.Close()
		return nil, fmt.Errorf("PublishersRegistry.Register error: %w", err)
	}

	return &Publisher[Payload]{
		topicPrefix: topicPrefix,
		unregister:  unregister,
		zmqContext:  zmqContext,
		zmqSocket:   zmqSocket,
	}, nil
}

func (p *Publisher[Payload]) Publish(_ context.Context, events []bus.Event[Payload]) error {
	for _, event := range events {
		buf := bytes.NewBuffer(p.topicPrefix)

		if len(event.EventKey) > 0 {
			buf.WriteString(string(event.EventKey))
		}
		buf.WriteByte(TopicAndPayloadDelimiter)

		msg, err := proto.Marshal(event.EventPayload)
		if err != nil {
			return fmt.Errorf("proto.Marshal error: %w", err)
		}

		_, err = buf.Write(msg)
		if err != nil {
			return fmt.Errorf("bytes.Buffer.Write error: %w", err)
		}

		_, err = p.zmqSocket.SendBytes(buf.Bytes(), 0)
		if err != nil {
			return fmt.Errorf("zmq4.Socket.SendBytes error: %w", err)
		}
	}

	return nil
}

func (p *Publisher[Payload]) Stop() error {
	err := p.unregister()
	if err != nil {
		return fmt.Errorf("unregister error: %w", err)
	}

	err = p.zmqSocket.Close()
	if err != nil {
		return fmt.Errorf("zmq.Socket.Close error: %w", err)
	}

	err = p.zmqContext.Term()
	if err != nil {
		return fmt.Errorf("zmq.Context.Term error: %w", err)
	}

	return nil
}
