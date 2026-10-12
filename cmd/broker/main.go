package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"log/slog"
	"time"

	"github.com/joho/godotenv"

	"github.com/lone4alker/tether/internal/broker"
	"github.com/lone4alker/tether/internal/models"
	"github.com/lone4alker/tether/internal/worker"
)

func startLeaseChecker(stopCh <-chan struct{}, bs *broker.BrokerServer) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			bs.StartLeaseChecker()
		}
	}
}

func main() {
	stopCh := make(chan struct{})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file loaded; using process environment", "error", err)
	}

	val := os.Getenv("TETHER_WAL_PATH")
	slog.Info("wal path resolved", "path", val)

	if val == "" {
		val = "tether.wal"
	}

	brokerServer, err := broker.NewBrokerServer(val)

	if err != nil {
		log.Fatalf("failed to start broker: %v", err)
	}

	p := func(j models.Job) error {
		return nil
	}
	workerPool := worker.NewWorkerPool(3, "http://localhost:8080", "email_job", p, 0)

	workerPool.StartWorkerPool()

	go startLeaseChecker(stopCh, brokerServer)

	go func() {

		err := brokerServer.Start(":8080")

		if err != nil {
			log.Printf("Error from Server Startup: %s", err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit
	workerPool.StopWorkerPool()
	close(stopCh)

	if err := brokerServer.Close(); err != nil {
		log.Printf("error closing broker: %v", err)
	}

}
