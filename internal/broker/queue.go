package broker

import (
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lone4alker/tether/internal/models"
)

var ErrJobNotFound = errors.New("No Job found in Queue")
var ErrLeaseNotRenewable = errors.New("cannot renew lease not in progress")
var ErrEmptyQueue = errors.New("queue is empty")
var ErrJobNotInProgress = errors.New("job is not in progress")
var ErrNoJobAvailable = errors.New("No Jobs Available")

type Queue struct {
	q   map[string][]models.Job
	dlq map[string][]models.Job
	mu  sync.RWMutex
}

func NewQueue() *Queue {
	q := make(map[string][]models.Job)
	dq := make(map[string][]models.Job)
	return &Queue{
		q:   q,
		dlq: dq,
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
				if jobs[i].Status == models.StatusPending && time.Now().After(jobs[i].NextRunAt) {
					jobs[i].Status = models.StatusInProgress
					now := time.Now()
					jobs[i].StartedAt = now
					// check if there's a LeaseDuration
					if jobs[i].LeaseDuration == 0 {
						jobs[i].LeaseDuration = 30 * time.Second // this is a sample time
					}
					jobs[i].LeaseExpiresAt = now.Add(jobs[i].LeaseDuration)
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
					jobs[i].RetryCount = job.RetryCount
					return jobs[i], nil
				}
			}
			return j, errors.New("Job with ID: " + job.Id.String() + " not found in Queue")
		}
	}

	return j, ErrJobNotFound
}
func applyFailOrRetry(job *models.Job) bool {
	if job.RetryCount >= job.MaxRetries {
		job.Status = models.StatusFailed
		return true
	} else {

		shift := min(job.RetryCount, 20)
		delay := time.Duration(1<<shift) * time.Second
		const maxDelay = 3 * time.Minute

		delay = min(delay, maxDelay)

		jitter := time.Duration(rand.Float64() * float64(delay) * 0.5)
		backoff := delay + jitter

		job.Status = models.StatusPending
		job.NextRunAt = time.Now().Add(backoff)
		job.RetryCount++
		job.LeaseExpiresAt = time.Time{}
		job.StartedAt = time.Time{}
		return false
	}
}

// method to check for expired leases
func (qu *Queue) CheckForExpiredLeases() {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for k, jobs := range qu.q {
		survivors := make([]models.Job, 0, len(jobs))
		for i := range jobs {
			if time.Now().After(jobs[i].LeaseExpiresAt) && jobs[i].Status == models.StatusInProgress {
				updated := jobs[i]
				dead := applyFailOrRetry(&updated)

				if dead {
					jobs[i] = updated
					qu.dlq[k] = append(qu.dlq[k], jobs[i])
				} else {
					jobs[i] = updated
					survivors = append(survivors, jobs[i])
				}
			} else {
				survivors = append(survivors, jobs[i])
			}
		}

		qu.q[k] = survivors
	}
}

// lease renewal method
func (qu *Queue) LeaseRenewal(queueName string, jobID uuid.UUID) (models.Job, error) {
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
				if jobs[i].Id == jobID {
					if jobs[i].Status == models.StatusInProgress {
						jobs[i].LeaseExpiresAt = time.Now().Add(jobs[i].LeaseDuration)
						return jobs[i], nil
					} else {
						return j, ErrLeaseNotRenewable

					}
				}
			}
			return j, errors.New("Job with ID: " + jobID.String() + " not found in Queue")
		}
	}

	return j, ErrJobNotFound

}

func (qu *Queue) FailOrRetry(jobID uuid.UUID) (*models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for k, jobs := range qu.q {
		for i := range jobs {
			if jobs[i].Id == jobID {
				if jobs[i].Status != models.StatusInProgress {
					return nil, ErrJobNotInProgress
				}
				updated := jobs[i]
				dead := applyFailOrRetry(&updated)

				if dead {
					deadJob := updated

					qu.q[k] = slices.Delete(jobs, i, i+1)
					qu.dlq[k] = append(qu.dlq[k], deadJob)

					return &deadJob, nil

				}

				jobs[i] = updated

				return &jobs[i], nil

			}
		}
	}

	return nil, ErrJobNotFound
}

// dlq Operations
func (qu *Queue) ListDeadLetter(queueName string) ([]models.Job, error) {
	qu.mu.RLock()
	defer qu.mu.RUnlock()

	jobs := qu.dlq[queueName]
	out := make([]models.Job, len(jobs))
	copy(out, jobs)
	return out, nil
}

func (qu *Queue) RedriveJob(queueName string, jobId uuid.UUID) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	var j models.Job

	// find the job
	dlqJobs := qu.dlq[queueName]

	for i := range dlqJobs {
		if dlqJobs[i].Id == jobId {
			dlqJobs[i].Status = models.StatusPending
			dlqJobs[i].RetryCount = 0
			dlqJobs[i].NextRunAt = time.Time{}

			redriveJob := dlqJobs[i]
			qu.q[queueName] = append(qu.q[queueName], redriveJob)
			qu.dlq[queueName] = slices.Delete(dlqJobs, i, i+1)
			return redriveJob, nil
		}
	}

	return j, ErrJobNotFound
}

func (qu *Queue) DiscardDeadJob(queueName string, jobId uuid.UUID) error {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	dlqJobs := qu.dlq[queueName]

	for i := range dlqJobs {
		if dlqJobs[i].Id == jobId {
			qu.dlq[queueName] = slices.Delete(dlqJobs, i, i+1)
			return nil
		}
	}

	return ErrJobNotFound
}