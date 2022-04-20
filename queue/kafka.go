package queue

import (
	"context"
	"encoding/json"
	kafka "github.com/segmentio/kafka-go"
	"log"
	"os"
	"postgresProject/dao"
	"postgresProject/db/repository"
	"strconv"
)

const (
	defaultKafkaAddress       = "localhost:9092"
	defaultKafkaPartition     = 0
	defaultKafkaWriteDeadline = 10
	defaultKafkaReadDeadline  = 10
	defaultKafkaReaderMin     = 1000    // 10KB
	defaultKafkaReaderMax     = 1000000 // 10MB

	kafkaAddressKey       = "KAFKA_ADDRESS"
	kafkaPartitionKey     = "KAFKA_PARTITION"
	kafkaWriteDeadlineKey = "KAFKA_WRITE_DEADLINE"
	kafkaReadDeadlineKey  = "KAFKA_READ_DEADLINE"
	kafkaReaderMinKey     = "KAFKA_READER_MIN"
	kafkaReaderMaxKey     = "KAFKA_READER_MAX"

	UserTopic = "user-topic"
)

var address = defaultKafkaAddress
var partition = defaultKafkaPartition
var readerMin = defaultKafkaReaderMin
var readerMax = defaultKafkaReaderMax
var kafkaRepository *repository.UserRepository

func InitKafka(rep *repository.UserRepository) {
	if v, exists := os.LookupEnv(kafkaAddressKey); exists {
		address = v
	}

	if v, exists := os.LookupEnv(kafkaPartitionKey); exists {
		intV, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Environment contains incorrect kafka-partition format: %s\n", v)
			log.Printf("Using default kafka-partition: %v\n", partition)
		} else {
			partition = intV
		}
	}

	if v, exists := os.LookupEnv(kafkaReaderMinKey); exists {
		intV, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Environment contains incorrect kafka-reader-min format: %s\n", v)
			log.Printf("Using default kafka-reader-min: %v\n", readerMin)
		} else {
			readerMin = intV
		}
	}

	if v, exists := os.LookupEnv(kafkaReaderMaxKey); exists {
		intV, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Environment contains incorrect kafka-reader-max format: %s\n", v)
			log.Printf("Using default kafka-reader-max: %v\n", readerMax)
		} else {
			readerMax = intV
		}
	}

	kafkaRepository = rep
	go receiveUserMessage(context.Background())
}

func SendMessage(ctx context.Context, topic string, message []byte) error {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(address),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("Error during closing writer: %s\n", err)
		}
	}()

	if err := w.WriteMessages(ctx, kafka.Message{Value: message}); err != nil {
		log.Printf("Error during sending message: %s\n", err)
		return err
	}
	return nil
}

func receiveUserMessage(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{address},
		Topic:     UserTopic,
		Partition: partition,
		MinBytes:  readerMin,
		MaxBytes:  readerMax,
	})

	for {
		message, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error during reading user message:%s", err)
			break
		}
		user := &dao.User{}
		err = json.Unmarshal(message.Value, user)
		if err != nil {
			log.Printf("Error during unmarshaling received user message body: %s\n", err)
			break
		}

		err = kafkaRepository.AddUser(*user)
		if err != nil {
			break
		}
	}

	if err := r.Close(); err != nil {
		log.Printf("Error during closing reader: %s\n", err)
	}
}
