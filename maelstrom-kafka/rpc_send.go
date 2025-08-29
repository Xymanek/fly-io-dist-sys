package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type sendRequest struct {
	//Type    string `json:"type"`
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type sendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (kafkaNode *KafkaNode) registerSendHandler() {
	kafkaNode.maelstromNode.Handle("send", func(msg maelstrom.Message) error {
		var request sendRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		log := kafkaNode.getOrCreateLog(request.Key)

		log.messagesLock.Lock()
		defer log.messagesLock.Unlock()

		log.messages = append(log.messages, request.Message)

		return kafkaNode.maelstromNode.Reply(msg, sendResponse{
			Type:   "send_ok",
			Offset: len(log.messages) - 1,
		})
	})
}
