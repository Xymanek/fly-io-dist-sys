package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kafkaNode := KafkaNode{
		maelstromNode: n,
		logs:          make(map[string]*KafkaLog),
	}

	kafkaNode.registerSendHandler()
	kafkaNode.registerPollHandler()
	kafkaNode.registerCommitOffsetsHandler()
	kafkaNode.registerListCommittedOffsetsHandler()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
