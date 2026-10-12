package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lone4alker/tether/internal/broker"
	"github.com/lone4alker/tether/internal/models"
	"github.com/lone4alker/tether/internal/worker"
)

func main() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	brokerServer := broker.NewBrokerServer()

	p := func(j models.Job) error {
		return nil
	}
	workerPool := worker.NewWorkerPool(3, "http://localhost:8080", "email_job", p)

	workerPool.StartWorkerPool()

	go func() {

		err := brokerServer.Start(":8080")

		if err != nil {
			log.Printf("Error from Server Startup: %s", err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit
	workerPool.StopWorkerPool()

}
