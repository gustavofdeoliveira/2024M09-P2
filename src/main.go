package main

import (
	"context"
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

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
		Value:          []byte(string(message)),
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
			inserToMongo(msg.Value)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
}

func inserToMongo(data []byte) error {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Erro ao carregar o arquivo .env")
	}

	// Recuperar usuário e senha do arquivo .env
	mongoUser := os.Getenv("MONGO_USER")
	mongoPassword := os.Getenv("MONGO_PASSWORD")

	// Use the SetServerAPIOptions() method to set the version of the Stable API on the client
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb+srv://%s:%s@prodan.qopo9c4.mongodb.net/?retryWrites=true&w=majority&appName=prodan", mongoUser, mongoPassword)).SetServerAPIOptions(serverAPI)

	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	coll := client.Database("db_prodan").Collection("prova")
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		log.Fatal(err)
	}
	json.Unmarshal([]byte(data), &v)
	result, err := coll.InsertOne(context.TODO(), v)
	if err != nil {
		panic(err)
	}

	print(result)

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
