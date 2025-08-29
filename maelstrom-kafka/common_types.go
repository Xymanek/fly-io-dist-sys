package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaNode struct {
	maelstromNode *maelstrom.Node
	store         *maelstrom.KV
}

type basicResponse struct {
	Type string `json:"type"`
}
