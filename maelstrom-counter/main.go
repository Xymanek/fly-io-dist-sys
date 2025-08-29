package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const counterKey = "counter"

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	// TODO
	//node.Handle("add", func(msg maelstrom.Message) error {
	//	// Unmarshal the message body as an loosely-typed map.
	//	var body map[string]any
	//	if err := json.Unmarshal(msg.Body, &body); err != nil {
	//		return err
	//	}
	//
	//	kv.CompareAndSwap(context.TODO(), counterKey, 0 /* TODO*/)
	//
	//	// Update the message type to return back.
	//	body["type"] = "add_ok"
	//
	//	// Echo the original message back with the updated message type.
	//	return node.Reply(msg, body)
	//})

	node.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"

		if value, err := kv.ReadInt(context.TODO(), counterKey); err != nil {
			return err
		} else {
			body["value"] = value
		}

		// Echo the original message back with the updated message type.
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
