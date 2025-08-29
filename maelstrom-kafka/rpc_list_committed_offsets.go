package main

import (
	"encoding/json"
	"errors"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type listCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func (kafkaNode *KafkaNode) registerListCommittedOffsetsHandler() {
	kafkaNode.maelstromNode.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var request listCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		offsets := make(map[string]int)
		for _, key := range request.Keys {
			committedOffset, err := kafkaNode.readCommittedOffset(key)
			if err != nil {
				var rpcError *maelstrom.RPCError
				ok := errors.As(err, &rpcError)

				// From requirements:
				// Keys that do not exist on the node can be omitted.
				if ok && rpcError.Code == maelstrom.KeyDoesNotExist {
					continue
				}

				return err
			}

			offsets[key] = committedOffset
		}

		return kafkaNode.maelstromNode.Reply(msg, listCommittedOffsetsResponse{
			Type:    "list_committed_offsets_ok",
			Offsets: offsets,
		})
	})
}
