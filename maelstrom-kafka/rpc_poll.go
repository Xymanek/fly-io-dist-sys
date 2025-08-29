package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type pollRequest struct {
	//Type    string           `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type pollResponse struct {
	Type     string             `json:"type"`
	Messages map[string][][]int `json:"msgs"`
}

// This message requests that a node return messages from a set of logs starting from the given offset in each log.

func (kafkaNode *KafkaNode) registerPollHandler() {
	kafkaNode.maelstromNode.Handle("poll", func(msg maelstrom.Message) error {
		var request pollRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		messages := make(map[string][][]int)

		for key, offset := range request.Offsets {
			messages[key] = kafkaNode.collectMessagesFromLogWithOffset(key, offset)
		}

		return kafkaNode.maelstromNode.Reply(msg, pollResponse{
			Type:     "poll_ok",
			Messages: messages,
		})
	})
}

func (kafkaNode *KafkaNode) collectMessagesFromLogWithOffset(key string, offset int) [][]int {
	log := kafkaNode.getOrCreateLog(key)

	log.messagesLock.Lock()
	defer log.messagesLock.Unlock()

	keyEntries := make([][]int, 0)
	for i, message := range log.messages {
		if i >= offset {
			keyEntries = append(keyEntries, []int{i, message})
		}
	}

	return keyEntries
}
