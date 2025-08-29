package main

import (
	"encoding/json"
	"errors"

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
			keyMessages, err := kafkaNode.collectMessagesFromLogWithOffset(key, offset)
			if err != nil {
				return err
			}

			messages[key] = keyMessages
		}

		return kafkaNode.maelstromNode.Reply(msg, pollResponse{
			Type:     "poll_ok",
			Messages: messages,
		})
	})
}

func (kafkaNode *KafkaNode) collectMessagesFromLogWithOffset(key string, offset int) ([][]int, error) {
	lastOffset, err := kafkaNode.readLastAllocatedIndex(key)
	if err != nil {
		return nil, err
	}

	keyEntries := make([][]int, 0)
	for index := offset; index < lastOffset; index++ {
		message, err := kafkaNode.readMessage(key, index)

		if err != nil {
			var rpcError *maelstrom.RPCError
			ok := errors.As(err, &rpcError)

			// From requirements:
			// These offsets can be sparse in that not every offset must contain a message.
			if ok && rpcError.Code == maelstrom.KeyDoesNotExist {
				continue
			}

			return nil, err
		}

		keyEntries = append(keyEntries, []int{index, message})
	}

	return keyEntries, nil
}
