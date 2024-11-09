package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

const KAFKA_CONNECT_URL = "kafka-service.kafka.svc.cluster.local:9092"

// KafkaConfig holds the Kafka configuration.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// Message represents the JSON message to be sent to Kafka.
type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var kafkaProducer sarama.SyncProducer
var kafkaConfig KafkaConfig

func initKafka() {
	kafkaConfig = KafkaConfig{
		Brokers: []string{KAFKA_CONNECT_URL},
		Topic:   "my-topic",
	}

	// Kafka producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Initialize the Kafka producer
	var err error
	kafkaProducer, err = sarama.NewSyncProducer(kafkaConfig.Brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
}

func handleKafkaMessage(w http.ResponseWriter, r *http.Request) {
	var msg Message
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	log.Println("Msg Key: ", msg.Key)
	log.Println("Msg Value: ", msg)
	// Prepare the Kafka message
	kafkaMsg := &sarama.ProducerMessage{
		Topic:     kafkaConfig.Topic,
		Key:       sarama.StringEncoder(msg.Key),
		Value:     sarama.StringEncoder(msg.Value),
		Timestamp: time.Now(),
	}

	// Send the message to Kafka
	partition, offset, err := kafkaProducer.SendMessage(kafkaMsg)
	if err != nil {
		fmt.Println(err)
		http.Error(w, fmt.Sprintf("Failed to send message to Kafka: %v", err), http.StatusInternalServerError)
		log.Println("Failed to send message to Kafka: ", http.StatusInternalServerError)
		return
	}

	// Respond with the partition and offset of the sent message
	log.Printf("Message sent to partition %d, offset %d\n", partition, offset)
	log.Printf("My new message")
}

func handleWebPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "color.html")
}

func main() {
	// Initialize Kafka producer
	initKafka()
	defer kafkaProducer.Close()

	// Define HTTP routes
	http.HandleFunc("/send", handleKafkaMessage)
	http.HandleFunc("/color", handleWebPage)

	// Start the HTTP server
	log.Println("Starting server on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
