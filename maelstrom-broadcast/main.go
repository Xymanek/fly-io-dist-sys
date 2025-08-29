package main

import (
	"encoding/json"
	"log"
	"slices"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n = maelstrom.NewNode()

type NeighbourContext struct {
	neighbourId string

	newValues          chan float64
	acknowledgedValues chan float64
}

var resendInterval, _ = time.ParseDuration("500ms")
var myNeighbours = make(map[string]NeighbourContext)

func neighbourPropagateLoop(context NeighbourContext) {
	pendingValues := make([]float64, 0)
	resendTimer := time.NewTicker(resendInterval)

	for {
		select {
		case <-resendTimer.C:
			log.Println("Timer trigger for ", context.neighbourId)
			sendValuesToNode(pendingValues, context.neighbourId)

		case newValue := <-context.newValues:
			pendingValues = append(pendingValues, newValue)

		case acknowledgedValue := <-context.acknowledgedValues:
			pendingValues = slices.DeleteFunc(pendingValues, func(existingValue float64) bool {
				return existingValue == acknowledgedValue
			})
		}
	}
}

func sendValuesToNode(values []float64, targetNode string) {
	log.Printf("Sending to node %s values (%v)", targetNode, values)

	body := make(map[string]any)
	body["type"] = "propagate"
	body["message"] = values

	err := n.Send(targetNode, body)
	if err != nil {
		panic(err)
	}
}

func main() {
	broadcastNewValueToNeighbours := func(newValue float64) {
		for _, context := range myNeighbours {
			//sendValuesToNode(newValue, neighbourId)
			context.newValues <- newValue
		}
	}

	seenValues := make([]float64, 0)

	handleIncomingValue := func(incomingValue float64) {
		// Ignore duplicates, we've stored and broadcasted them already
		for _, seenValue := range seenValues {
			if seenValue == incomingValue {
				return
			}
		}

		// Store the value
		seenValues = append(seenValues, incomingValue)

		// Send to other nodes
		broadcastNewValueToNeighbours(incomingValue)
	}

	n.Handle("propagate_ok", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		values := body["message"].([]interface{})

		// Store acknowledged
		for _, value := range values {
			myNeighbours[msg.Src].acknowledgedValues <- value.(float64)
		}

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Handle the value
		handleIncomingValue(body["message"].(float64))

		// Update the message type to return back.
		body["type"] = "broadcast_ok"
		delete(body, "message")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("propagate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Handle the value
		values := body["message"].([]interface{})
		for _, value := range values {
			handleIncomingValue(value.(float64))
		}

		// Update the message type to return back.
		body["type"] = "propagate_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"

		body["messages"] = seenValues

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Grab the topology map & store our neighboursItem
		topologyMap := body["topology"].(map[string]interface{})
		for nodeId, neighboursItem := range topologyMap {
			if nodeId == n.ID() {
				neighboursItemAsArray := neighboursItem.([]interface{})

				for _, item := range neighboursItemAsArray {
					neighbourId := item.(string)

					context := NeighbourContext{
						neighbourId,
						make(chan float64, 10),
						make(chan float64, 10),
					}

					myNeighbours[neighbourId] = context
					go neighbourPropagateLoop(context)

					log.Printf("Got neighbour %s", neighbourId)
				}
			}
		}

		// Update the message type to return back.
		body["type"] = "topology_ok"
		delete(body, "topology")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
