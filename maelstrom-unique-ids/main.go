package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// In prod this would be assigned by orchestrator/similar, but currently let's abuse the fact
	// that all replicas will be running under the same OS
	idPrefix := os.Getpid()

	var generatedIds atomic.Uint64

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		localId := generatedIds.Add(1)
		globalId := fmt.Sprintf("%d_%d", idPrefix, localId)

		body["id"] = globalId

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
