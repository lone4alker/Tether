package broker

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/lone4alker/tether/internal/models"
)

var ErrJobNotFound = errors.New("No Job found in Queue")
var ErrEmptyQueue = errors.New("queue is empty")
var ErrNoJobAvailable = errors.New("No Jobs Available")

type Queue struct {
	q  map[string][]models.Job
	mu sync.RWMutex
}

func NewQueue() *Queue {
	q := make(map[string][]models.Job)
	return &Queue{
		q: q,
	}
}

// Add Job
func (qu *Queue) AddJob(queueName string, job models.Job) error {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	qu.q[queueName] = append(qu.q[queueName], job)

	return nil
}

// read a job without modifying it
func (qu *Queue) ReadJobById(queueName string, jobId uuid.UUID) (models.Job, error) {
	qu.mu.RLock()
	defer qu.mu.RUnlock()

	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		for _, job := range jobs {
			if job.Id == jobId {
				return job, nil
			}
		}
	} else {
		return j, ErrEmptyQueue
	}

	return j, ErrEmptyQueue
}

// Request Job
func (qu *Queue) RequestJob(queueName string) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		if len(jobs) > 0 {
			for i := range jobs {
				if jobs[i].Status == models.StatusPending {
					jobs[i].Status = models.StatusInProgress
					return jobs[i], nil
				}
			}
		}

	}
	return j, ErrNoJobAvailable
}

// update job
func (qu *Queue) UpdateJob(queueName string, job models.Job) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		if len(jobs) == 0 {
			return j, ErrEmptyQueue
		} else {
			for i := range jobs {
				if jobs[i].Id == job.Id {
					// update the job
					jobs[i].Status = job.Status
					jobs[i].CompletedAt = job.CompletedAt
					return jobs[i], nil
				}
			}
			return j, errors.New("Job with ID: " + job.Id.String() + " not found in Queue")
		}
	}

	return j, ErrJobNotFound
}