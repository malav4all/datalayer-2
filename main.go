package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"
)

var (
	kafkaWriter *kafka.Writer
	rabbitConn  *amqp.Connection
	rabbitCh    *amqp.Channel
)

func main() {
	// Initialize RabbitMQ
	initRabbitMQ()

	q, err := rabbitCh.QueueDeclare(
		"json_data_queue", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare a queue: %s", err))
	}

	msgs, err := rabbitCh.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to register a consumer: %s", err))
	}

	// Initialize Kafka
	initKafka()

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			go handleDataPacket(d.Body)
		}
	}()

	fmt.Println("Waiting for JSON messages. To exit press CTRL+C")
	<-forever
}

func initKafka() {
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("103.20.213.153:9092"), // Use the provided Kafka broker address
		Topic:    "test",
		Balancer: &kafka.LeastBytes{},
	}

	// Test Kafka connection by sending a test message
	err := kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("test-key"),
			Value: []byte("test message"),
		},
	)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %s", err)
	} else {
		fmt.Println("Kafka is connected.")
	}
}

func initRabbitMQ() {
	var err error
	rabbitConn, err = amqp.Dial("amqp://admin:admin@103.20.214.74:5673/")
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %s", err))
	}

	rabbitCh, err = rabbitConn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open a channel: %s", err))
	}

	// Declare exchanges for live tracking and alerts
	err = rabbitCh.ExchangeDeclare(
		"live_tracking_exchange", // name
		"topic",                  // type
		true,                     // durable
		false,                    // auto-deleted
		false,                    // internal
		false,                    // no-wait
		nil,                      // arguments
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare live tracking exchange: %s", err))
	}

	err = rabbitCh.ExchangeDeclare(
		"alerts_exchange", // name
		"topic",           // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare alerts exchange: %s", err))
	}
}

func handleDataPacket(packet []byte) error {
	fmt.Println("Received a JSON data packet:", string(packet))

	// Send JSON data to Kafka
	err := sendToKafka(packet)
	if err != nil {
		return fmt.Errorf("error sending data to Kafka: %w", err)
	}

	// Parse JSON data to extract deviceId
	var data map[string]interface{}
	err = json.Unmarshal(packet, &data)
	if err != nil {
		return fmt.Errorf("error unmarshalling JSON data: %w", err)
	}

	deviceId, ok := data["deviceId"].(string)
	if !ok {
		return fmt.Errorf("deviceId not found in data")
	}

	// Send data to RabbitMQ exchanges
	err = sendToRabbitMQExchanges(packet, deviceId)
	if err != nil {
		return fmt.Errorf("error sending data to RabbitMQ exchanges: %w", err)
	}

	// Send data to webhook
	err = sendToWebhook(packet)
	if err != nil {
		return fmt.Errorf("error sending data to webhook: %w", err)
	}

	return nil
}

func sendToKafka(data []byte) error {
	err := kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
			Value: data,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	return nil
}

func sendToRabbitMQExchanges(data []byte, deviceId string) error {
	err := rabbitCh.Publish(
		"live_tracking_exchange", // exchange
		"track."+deviceId,        // routing key
		false,                    // mandatory
		false,                    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to live tracking exchange: %w", err)
	}

	err = rabbitCh.Publish(
		"alerts_exchange", // exchange
		"alert."+deviceId, // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to alerts exchange: %w", err)
	}

	return nil
}

func sendToWebhook(data []byte) error {
	webhookURL := "http://example.com/webhook" // Replace with your webhook URL
	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status: %s", resp.Status)
	}

	return nil
}
