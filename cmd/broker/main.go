package main

import (
	"log"

	"github.com/lone4alker/tether/internal/broker"
)

func main() {
	brokerServer := broker.NewBrokerServer()

	if err := brokerServer.Start(":8080"); err != nil {
		log.Fatalf("failed to start broker: %v", err)
	}
}
