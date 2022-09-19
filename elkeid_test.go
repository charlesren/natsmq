package natsmq

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	agentproto "github.com/bytedance/Elkeid/agent/proto"
	"github.com/bytedance/Elkeid/server/agent_center/common/ylog"
	pb "github.com/bytedance/Elkeid/server/agent_center/grpctrans/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/nats.go"
)

func Testreceive(t *testing.T) {
	logLevel := 1
	logPath := "/tmp/a.log"
	logger := ylog.NewYLog(
		ylog.WithLogFile(logPath),
		ylog.WithMaxAge(3),
		ylog.WithMaxSize(10),
		ylog.WithMaxBackups(3),
		ylog.WithLevel(logLevel),
	)
	ylog.InitLogger(logger)
	InitNats()
	sub, err := js.PullSubscribe(subjectRawData, durableName)
	if err != nil {
		log.Fatal(err)
	}
	//for {
	msgs, err := sub.Fetch(10, nats.MaxWait(30*time.Second))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
		}
		ylog.Infof("nats", "fetch error: %v\n", err.Error())
	}

	for _, msg := range msgs {
		fmt.Println("new msg >>>")
		m := MQMsgPool.Get().(*pb.MQData)
		err := proto.Unmarshal(msg.Data, m)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Msg type: %+v\n", m.DataType)
		t := PayloadPool.Get().(*agentproto.Payload)
		switch m.DataType {
		case 1000:
			fmt.Printf("Agent Stat\n")
		case 1001:
			fmt.Printf("Plgin Stat\n")
		case 1010:
			fmt.Printf("Plugin loger\n")
		case 5001:
			fmt.Printf("Plugin : Collector ===> Socket\n")
		case 5002:
			fmt.Printf("Plugin : Collector ===> User\n")
		case 5003:
			fmt.Printf("Plugin : Collector ===> Cron\n")
		case 5004:
			fmt.Printf("Plugin : Collector ===> Deb\n")
		case 5005:
			fmt.Printf("Plugin : Collector ===> rpm\n")
		case 5006:
			fmt.Printf("Plugin : Collector ===> pypi\n")
		case 5010:
			fmt.Printf("Plugin Collector ===> Systemd Unit\n")
		case 5011:
			fmt.Printf("Plugin Collector ===> Jar\n")
		default:
			fmt.Printf("Msg: %+v\n", m)
		}
		err = proto.Unmarshal(m.Body, t)
		fmt.Printf("Payload : %v\n", t.Fields)
		fmt.Printf("end ----\n")
		msg.Ack()
		PayloadPool.Put(t)
		MQMsgPool.Put(m)
	}
	//}
}
