package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestOpenFileSuccess(t *testing.T) {
	fmt.Println("TestOpenFileSuccess")

	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		t.Fatalf("Erro ao criar arquivo temporário: %s", err)
	}
	tmpfilePath := tmpfile.Name()

	defer os.Remove(tmpfilePath)
	tmpfile.Close()

	file := openFile(tmpfilePath)
	if file == nil {
		t.Errorf("openFile retornou nil para um arquivo existente")
	}
	file.Close()
}

func TestReadFileSuccess(t *testing.T) {
	fmt.Println("TestReadFileSuccess")
	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		t.Fatalf("Erro ao criar arquivo temporário: %s", err)
	}
	tmpfilePath := tmpfile.Name()

	// Cleanup: Garante que o arquivo temporário seja removido após o teste.
	defer os.Remove(tmpfilePath)
	tmpfile.Close()

	// Teste: Tenta abrir o arquivo temporário.
	file := openFile(tmpfilePath)
	if file == nil {
		t.Errorf("openFile retornou nil para um arquivo existente")
	}
	bytes := readFile(file)

	if bytes == nil {
		t.Errorf("readFile retornou nil para um arquivo existente")
	}
	file.Close()

}

func TestProducerConnection(t *testing.T) {
	fmt.Println("TestProducerConnection")

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "go-producer",
	})
	if err != nil {
		log.Fatalf("[PRODUCER] Falha ao criar produtor: %v", err)
	}
	defer producer.Close()
}

func TestConsumerConnection(t *testing.T) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
}

func TestKafkaPublicAndRecevedMessage(t *testing.T) {
	fmt.Println("TestKafkaPublicAndRecevedMessage")
	var file = openFile("./data/data.json")
	var bytes = readFile(file)

	var result []map[string]interface{}
	var err = json.Unmarshal(bytes, &result)
	if err != nil {
		t.Fatalf("Erro ao decodificar o JSON: %s", err)
	}
	for _, item := range result {
		item = createObject(item)
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:29092",
			"client.id":         "go-producer",
		})
		if err != nil {
			log.Fatalf("[PRODUCER] Falha ao criar produtor: %v", err)
		}
		defer producer.Close()

		topic := "test_test"

		message, err := json.Marshal(item)
		if err != nil {
			log.Fatalf("Error marshal")
		}
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(string(message)),
		}, nil)

		producer.Flush(10 * 1000)

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
		
		msg, err := consumer.ReadMessage(-1)
		if err == nil {

			if string(msg.Value) != string(message) {
				log.Fatal("Mensagens Não são iguais")

			}
		}
	}
}
