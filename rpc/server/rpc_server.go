package main

import (
	"log"
	"strconv"
	"time"

	common "github.com/itscaro/cloudamqp/common"
	"github.com/streadway/amqp"
	"os"
)

func server(id string) {
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
		"rpc_queue", // name
		false,       // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	common.FailOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,                  // queue name
		"rpc_queue_routing_key", // routing key
		"my.exchange",           // exchange
		false,
		nil)
	common.FailOnError(err, "Failed to bind a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	common.FailOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		id,     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	common.FailOnError(err, "Failed to register a consumer")

	log.Printf("[%s] [*] Awaiting RPC requests", id)

	for d := range msgs {
		//log.Printf("%+v", d)

		n, err := strconv.Atoi(string(d.Body))
		common.FailOnError(err, "Failed to convert body to integer")

		response := 0
		if n <= 40 {
			log.Printf("[%s] [>] fib(%d)", id, n)
			response = common.Fib(n)
			log.Printf("[%s] [<] fib(%d) => %d", id, n, response)
		}

		time.Sleep(5000 * time.Millisecond)
		// publish to default exchange to temporary queue created by client
		err = ch.Publish(
			"",        // exchange
			d.ReplyTo, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: d.CorrelationId,
				Body:          []byte(strconv.Itoa(response)),
			})
		common.FailOnError(err, "Failed to publish a message")

		d.Ack(false)
	}


}

func main() {
	var nbGoroutines int
	if (len(os.Args) < 2) || os.Args[1] == "" {
		nbGoroutines = 2
	} else {
		nbGoroutines, _ = strconv.Atoi(os.Args[1])
	}

	forever := make(chan bool)

	for i := 1; i <= nbGoroutines; i++ {
		go server("server " + strconv.Itoa(i))
	}

	<-forever
}
