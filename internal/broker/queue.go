package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrJobNotFound = errors.New("No Job found in Queue")
var ErrEmptyQueue = errors.New("queue is empty")
var ErrNoJobAvailable = errors.New("No Jobs Available")

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusInProgress JobStatus = "in_progress"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	Id          uuid.UUID
	Name        string
	Payload     map[string]any
	Status      JobStatus
	CreatedAt   time.Time
	CompletedAt time.Time
}

type Queue struct {
	q  map[string][]Job
	mu sync.RWMutex
}

func NewQueue() *Queue {
	q := make(map[string][]Job)
	return &Queue{
		q: q,
	}
}

// Add Job
func (qu *Queue) AddJob(queueName string, job Job) error {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	qu.q[queueName] = append(qu.q[queueName], job)

	return nil
}

// read a job without modifying it
func (qu *Queue) ReadJobById(queueName string, jobId uuid.UUID) (Job, error) {
	qu.mu.RLock()
	defer qu.mu.RUnlock()

	jobs, ok := qu.q[queueName]
	var j Job
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
func (qu *Queue) RequestJob(queueName string) (Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	jobs, ok := qu.q[queueName]
	var j Job
	if ok {
		if len(jobs) > 0 {
			for i := range jobs {
				if jobs[i].Status == StatusPending {
					jobs[i].Status = StatusInProgress
					return jobs[i], nil
				}
			}
		}

	}
	return j, ErrNoJobAvailable
}

// update job
func (qu *Queue) UpdateJob(queueName string, job Job) (Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	jobs, ok := qu.q[queueName]
	var j Job
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