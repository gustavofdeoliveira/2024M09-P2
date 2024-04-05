package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func openFile(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Erro ao abrir o arquivo: %s", err)
	}
	return file
}

func readFile(file *os.File) []byte {
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Erro ao ler o arquivo: %s", err)
	}
	return bytes
}

func createObject(result map[string]interface{}) map[string]interface{} {
	newItem := make(map[string]interface{})
	for key, value := range result {
		if key == "timestamp" {
			newItem[key] = time.Now().Format(time.RFC3339)
		} else {
			switch v := value.(type) {
			case float64:
				newItem[key] = v * rand.Float64()
			default:
				newItem[key] = value
			}
		}
	}
	return newItem
}

func kafka_producer(msg map[string]interface{}) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "go-producer",
	})
	if err != nil {
		log.Fatalf("[PRODUCER] Falha ao criar produtor: %v", err)
	}
	defer producer.Close()

	topic := "qualidadeAr"
	fmt.Printf("[PRODUCER] Conectado ao tópico %s...\n", topic)

	message, err := json.Marshal(msg)

	if err != nil {
		log.Fatalf("[PRODUCER] Falha ao codificar a mensagem: %v", err)
	}

	fmt.Printf("[PRODUCER] Parsed message: %s\n", string(message))

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)

	producer.Flush(10 * 1000)
	kafka_consumer(topic)
}

func kafka_consumer(topic string) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("[CONSUMER] Received message: %s\n", string(msg.Value))
			var result map[string]interface{}
			json.Unmarshal(msg.Value, &result)
			for key, value := range result {
				key = strings.ToUpper(key)
				fmt.Printf("[CONSUMER] %v: %v\n", key, value)
			}
			generateParquet(result)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
}

type result struct {
	message  string
	datetime string
}

func generateParquet(data map[string]interface{}) error {
	log.Println("[PARQUET] generating parquet file")
	fw, err := local.NewLocalFileWriter("./parquet/output.parquet")
	if err != nil {
		return err
	}

	pw, err := writer.NewParquetWriter(fw, new(result), int64(len(data)))
	if err != nil {
		return err
	}
	defer fw.Close()
	message, err := json.Marshal(data)

	if err != nil {
		log.Fatalf("[PARQUET] Erro ao decodificar o JSON: %s", err)
	}

	if err = pw.Write(string(message)); err != nil {
		return err
	}

	if err = pw.WriteStop(); err != nil {
		return err
	}
	log.Println("[PARQUET] Finshed!")
	return nil
}

func main() {

	var file = readFile(openFile("./data/data.json"))
	result := []map[string]interface{}{}
	var err = json.Unmarshal(file, &result)
	if err != nil {
		log.Fatalf("Erro ao decodificar o JSON: %s", err)
	}
	for _, item := range result {
		kafka_producer(createObject(item))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("Encerrando o programa.")
}
