package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	common "github.com/itscaro/cloudamqp/common"
	"github.com/streadway/amqp"
)

func fibonacciRPC(n int) (res int, err error) {
	conn, err := amqp.Dial("amqp://itscaro:itscaro@localhost:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"my.exchange", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	common.FailOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	common.FailOnError(err, "Failed to declare a queue")

	corrId := common.RandomString(32)

	msgs, err := ch.Consume(
		q.Name,           // queue
		"client-"+corrId, // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	common.FailOnError(err, "Failed to register a consumer")

	err = ch.Publish(
		"my.exchange",           // exchange
		"rpc_queue_routing_key", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(strconv.Itoa(n)),
		})
	common.FailOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			common.FailOnError(err, "Failed to convert body to integer")
			break
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	n := common.BodyFrom(os.Args)

	for i := range common.MergeChannels(request(n)) {
		fmt.Println(i)
	}
	/*
		ch1 := request(n)
		ch2 := request(n)
		ch3 := request(n)
		ch4 := request(n)
		ch5 := request(n)

		for i := range merge(ch1, ch2, ch3, ch4, ch5) {
			fmt.Println(i)
		}
	*/
}

func request(n int) <-chan int {
	ch := make(chan int)

	go func() {
		log.Printf(" [x] Requesting fib(%d)", n)
		res, err := fibonacciRPC(n)
		common.FailOnError(err, "Failed to handle RPC request")
		log.Printf(" [.] Got %d", res)

		ch <- res
		close(ch)
	}()

	return ch
}
