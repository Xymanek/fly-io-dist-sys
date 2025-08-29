package main

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func (kafkaNode *KafkaNode) allocateNextMessageIndex(logKey string) int {
	storeKey := getLastAllocatedIndexKey(logKey)

	// Loop forever until success
	for {
		newIndex, err := attemptAllocateNextMessageIndex(kafkaNode.store, storeKey)

		if err != nil {
			log.Println("Failed to allocate next message index, retrying:", err)
			continue
		}

		return newIndex
	}
}

func attemptAllocateNextMessageIndex(store *maelstrom.KV, storeKey string) (int, error) {
	ctx := context.TODO()

	// This could be simply store.CompareAndSwap(ctx, storeKey, 0, 1, true)
	// but the common case is for some value to already exist
	lastValue, err := store.ReadInt(ctx, storeKey)
	if err != nil {
		var rpcError *maelstrom.RPCError
		ok := errors.As(err, &rpcError)

		if ok && rpcError.Code == maelstrom.KeyDoesNotExist {
			log.Println("Attempting to allocate the first index for log key ", storeKey)
			return 1, store.CompareAndSwap(ctx, storeKey, -99, 1, true)
		}

		return lastValue, err
	}

	log.Println("Got last value for log key ", storeKey, " - ", lastValue, " - attempting to increment")

	newValue := lastValue + 1
	return newValue, store.CompareAndSwap(ctx, storeKey, lastValue, newValue, false)
}

func (kafkaNode *KafkaNode) readLastAllocatedIndex(logKey string) (int, error) {
	storeKey := getLastAllocatedIndexKey(logKey)
	ctx := context.TODO()

	return kafkaNode.store.ReadInt(ctx, storeKey)
}

func getLastAllocatedIndexKey(logKey string) string {
	return buildStoreKey(logKey, "lastAllocatedIndex")
}

func (kafkaNode *KafkaNode) writeMessage(logKey string, index int, messageValue int) error {
	storeKey := getIndividualMessageKey(logKey, index)
	ctx := context.TODO()

	return kafkaNode.store.Write(ctx, storeKey, messageValue)
}

func (kafkaNode *KafkaNode) readMessage(logKey string, index int) (message int, error error) {
	storeKey := getIndividualMessageKey(logKey, index)
	ctx := context.TODO()

	return kafkaNode.store.ReadInt(ctx, storeKey)
}

func getIndividualMessageKey(logKey string, index int) string {
	return buildStoreKey(logKey, "message", strconv.Itoa(index))
}

func (kafkaNode *KafkaNode) readCommittedOffset(logKey string) (int, error) {
	storeKey := getCommittedOffsetKey(logKey)
	ctx := context.TODO()

	return kafkaNode.store.ReadInt(ctx, storeKey)
}

// Note: currently this allows going back in time, but I didn't see any requirement to forbid this anywhere.
// If such need ever comes up, replace this with read + CAS.
func (kafkaNode *KafkaNode) writeCommittedOffset(logKey string, newCommittedOffset int) error {
	storeKey := getCommittedOffsetKey(logKey)
	ctx := context.TODO()

	return kafkaNode.store.Write(ctx, storeKey, newCommittedOffset)
}

func getCommittedOffsetKey(logKey string) string {
	return buildStoreKey(logKey, "committed_offset")
}

func buildStoreKey(parts ...string) string {
	return strings.Join(parts, ";;;")
}
