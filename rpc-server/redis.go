package main

// cd rpc-server
// run "go mod download github.com/redis/go-redis/v9"
// run "go get github.com/redis/go-redis/v9"
import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redis.Client
}

// InitClient to initialise client
func (c *RedisClient) InitClient(ctx context.Context, address, password string) error {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       0,        // use default db
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}

	c.cli = client
	return nil
}

// Message structures the main components of a message that is to be stored in Redis as JSON format
// Specify JSON mapping fields
type Message struct {
	Sender    string `json:"sender"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// SaveMessage to insert into Redis db
func (c *RedisClient) SaveMessage(ctx context.Context, roomID string, message *Message) error {
	text, err := json.Marshal(message) // text to contain JSON type message
	if err != nil {
		return err
	}

	// message instance
	member := &redis.Z{
		Score:  float64(message.Timestamp), // extract message timestamp
		Member: text,                       // message data stored as JSON
	}

	// insert message to Redis
	_, err = c.cli.ZAdd(ctx, roomID, *member).Result()
	if err != nil {
		return err
	}

	return nil
}

// GetMessagesByRoomID to retrieve all message logs in a particular room for two users
func (c *RedisClient) GetMessagesByRoomID(ctx context.Context, roomID string, start, end int64, getReverse bool) ([]*Message, error) {
	var (
		rawMessages []string
		messages    []*Message
		err         error
	)

	if getReverse {
		// DESC: first message is the latest message
		rawMessages, err = c.cli.ZRevRange(ctx, roomID, start, end).Result()
		if err != nil {
			return nil, err
		}
	} else {
		// ASC: first message is the earliest message
		rawMessages, err = c.cli.ZRange(ctx, roomID, start, end).Result()
		if err != nil {
			return nil, err
		}
	}

	// unmarshal all extracted JSON messages to struct
	for _, msg := range rawMessages {
		temp := &Message{}
		err := json.Unmarshal([]byte(msg), temp)
		if err != nil {
			return nil, err
		}
		messages = append(messages, temp)
	}

	return messages, nil
}
