package broker

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
)

type BrokerServer struct {
	queue *Queue
}

// POST /queues/{queueName}/jobs - AddJob
// GET  /queues/{queueName}/jobs - RequestJob

func NewBrokerServer() *BrokerServer {
	q := NewQueue()

	return &BrokerServer{queue: q}
}

func (bs *BrokerServer) AddJob(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	var job Job

	err := json.NewDecoder(r.Body).Decode(&job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newUUID := uuid.New()
	job.Id = newUUID
	job.Status = StatusPending

	err = bs.queue.AddJob(queueName, job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}

func (bs *BrokerServer) RequestJob(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")

	job, err := bs.queue.RequestJob(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)

}

func (bs *BrokerServer) Start(port string) error {
	http.HandleFunc("POST /queues/{queueName}/jobs", bs.AddJob)
	http.HandleFunc("GET /queues/{queueName}/jobs", bs.RequestJob)

	err := http.ListenAndServe(port, nil)

	if err != nil {
		return err
	}

	log.Printf("Server started on Port %s", port)

	return nil
}
