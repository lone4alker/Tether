package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/lone4alker/tether/internal/models"
)

var NotFoundError = errors.New("job Id not found")
var BadRequestError = errors.New("bad request")

type WorkerState string

const (
	Idle       WorkerState = "idle"
	Busy       WorkerState = "busy"
	Terminated WorkerState = "terminated"
)

type Worker struct {
	Id             uuid.UUID
	State          WorkerState
	BrokerEndpoint string
	Process        func(models.Job) error
	JobType        string
	logger         *slog.Logger
}

func NewWorker(brokerEndpoint string, proccesFunc func(models.Job) error, jobType string) *Worker {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return &Worker{
		Id:             uuid.New(),
		State:          Idle,
		BrokerEndpoint: brokerEndpoint,
		Process:        proccesFunc,
		JobType:        jobType,
		logger:         logger,
	}

}

func (w *Worker) Start(stopCh <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			func() {
				// make http GET Request here
				endpoint := fmt.Sprintf("%s/queues/%s/jobs", w.BrokerEndpoint, w.JobType)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				client := &http.Client{}
				resp, err := client.Do(req)

				if err != nil {
					// handle error
					w.logger.Error("failed to fetch job from broker", "error", err, "job_type", w.JobType)
					return
				}

				defer resp.Body.Close()

				if resp.StatusCode == http.StatusNotFound {
					w.logger.Debug("no job available", "job_type", w.JobType)
					return
				}

				var job models.Job

				// Decode the JSON response
				if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
					w.logger.Error("failed to decode job response", "error", err, "job_type", w.JobType)
					return
				}

				w.logger.Info("job received", "job_id", job.Id, "job_type", w.JobType)

				//else -> Process and Update the broker
				w.ProcessJob(job)
			}()
		}
	}
}

func (w *Worker) updateJobRequest(job models.Job) error {
	updateEndpoint := fmt.Sprintf("%s/queues/%s/jobs/%s", w.BrokerEndpoint, w.JobType, job.Id)

	bodyBytes, err := json.Marshal(job)

	if err != nil {
		return fmt.Errorf("error marshalling bodybytes: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	updateBody := bytes.NewBuffer(bodyBytes)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, updateEndpoint, updateBody)

	if err != nil {
		return fmt.Errorf("error making http put request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		return fmt.Errorf("error, request failed: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusBadRequest {
		return BadRequestError
	}

	if res.StatusCode == http.StatusNotFound {
		return NotFoundError
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("unexpected status: %s", res.Status)
	}

	if _, err := io.ReadAll(res.Body); err != nil {
		return fmt.Errorf("error %w", err)
	}

	w.logger.Info("job update accepted by broker", "job_id", job.Id, "job_type", w.JobType, "status", res.Status)

	return nil
}

func (w *Worker) ProcessJob(job models.Job) {
	if err := w.Process(job); err != nil {
		w.logger.Error("job failed", "error", err, "job_id", job.Id, "job_type", w.JobType)
		return
	}

	job.Status = models.StatusCompleted
	job.CompletedAt = time.Now()

	if updateErr := w.updateJobRequest(job); updateErr != nil {
		w.logger.Error("failed to update completed job on broker", "error", updateErr, "job_id", job.Id, "job_type", w.JobType)
	}
}
