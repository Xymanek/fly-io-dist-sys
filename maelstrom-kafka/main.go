package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kafkaNode := KafkaNode{
		maelstromNode: n,
		store:         maelstrom.NewLinKV(n),
	}

	kafkaNode.registerSendHandler()
	kafkaNode.registerPollHandler()
	kafkaNode.registerCommitOffsetsHandler()
	kafkaNode.registerListCommittedOffsetsHandler()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
