package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type commitOffsetsRequest struct {
	//Type    string           `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func (kafkaNode *KafkaNode) registerCommitOffsetsHandler() {
	kafkaNode.maelstromNode.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var request commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		for key, offset := range request.Offsets {
			kafkaNode.commitMessagesInLogWithOffset(key, offset)
		}

		return kafkaNode.maelstromNode.Reply(msg, basicResponse{
			Type: "commit_offsets_ok",
		})
	})
}

func (kafkaNode *KafkaNode) commitMessagesInLogWithOffset(key string, offset int) {
	log := kafkaNode.getOrCreateLog(key)

	log.acknowledgedOffsetLock.Lock()
	defer log.acknowledgedOffsetLock.Unlock()

	log.acknowledgedOffset = max(log.acknowledgedOffset, offset)
}
