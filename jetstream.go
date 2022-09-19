package natsmq

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

// createStream 使用JetStreamContext创建流
func createStream(js nats.JetStreamContext, sc *nats.StreamConfig) error {
	// Check if the stream already exists; if not, create it.
	info, err := js.StreamInfo(sc.Name)
	if err != nil {
		log.Println(err)
	}
	if info == nil {
		log.Printf("creating stream %q and subjects %q", sc.Name, sc.Subjects)
		info, err = js.AddStream(sc)
		if err != nil {
			return err
		}
	}
	marshal, _ := json.Marshal(info)
	fmt.Println("===> StreamInfo ", string(marshal))
	return nil
}

// createConsumer 使用JetStreamContext创建流
func createConsumer(js nats.JetStreamContext, stream string, durable string, cc *nats.ConsumerConfig) error {
	// Check if the consumer already exists; if not, create it.
	info, err := js.ConsumerInfo(stream, durable)
	if err != nil {
		log.Println(err)
	}
	if info == nil {
		log.Printf("creating consumer for stream: %s ,durable : %s\n", stream, durable)
		info, err = js.AddConsumer(stream, cc)
		if err != nil {
			return err
		}
	}
	marshal, _ := json.Marshal(info)
	fmt.Println("===> ConsumerInfo ", string(marshal))
	return nil
}
