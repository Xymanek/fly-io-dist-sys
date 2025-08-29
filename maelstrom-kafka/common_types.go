package main

import (
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaNode struct {
	maelstromNode *maelstrom.Node

	logs map[string]*KafkaLog
}

type KafkaLog struct {
	messagesLock sync.Mutex

	// TODO: What does the following imply:
	// These offsets can be sparse in that not every offset must contain a message.
	messages []int

	acknowledgedOffsetLock sync.Mutex
	acknowledgedOffset     int
}

type basicResponse struct {
	Type string `json:"type"`
}

func (kafkaNode *KafkaNode) getOrCreateLog(key string) *KafkaLog {
	log, ok := kafkaNode.logs[key]

	if !ok {
		log = &KafkaLog{}
		kafkaNode.logs[key] = log
	}

	return log
}
