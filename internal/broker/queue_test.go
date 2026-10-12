package broker

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	return NewQueue()
}

func createJob() Job {
	return Job{
		Id:        uuid.New(),
		Name:      "test Job",
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}
}

func TestAddJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()

	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	jobs, _ := q.q["email"]
	got := len(jobs)
	want := 1

	if got != want {
		t.Errorf("got %d, wanted %d", got, want)
	}

	if jobs[0].Id.String() != j.Id.String() {
		t.Errorf("got %q, wanted %q", jobs[0].Id.String(), j.Id.String())
	}

	if jobs[0].Name != j.Name {
		t.Errorf("got %q, wanted %q", jobs[0].Name, j.Name)
	}

	if jobs[0].Status != j.Status {
		t.Errorf("got %v, wanted %v", jobs[0].Status, j.Status)
	}

	if jobs[0].CreatedAt != j.CreatedAt {
		t.Errorf("got %v, wanted %v", jobs[0].CreatedAt, j.CreatedAt)
	}
}

func TestRequestJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()
	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	res, err := q.RequestJob("email")

	if res.Status != StatusInProgress {
		t.Errorf("got %v, wanted %v", res.Status, StatusInProgress)
	}

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRequestJobEmptyQueue(t *testing.T) {
	q := newTestQueue(t)
	expectedErrMsg := ErrNoJobAvailable.Error()

	_, err := q.RequestJob("email")

	if err.Error() != expectedErrMsg {
		t.Errorf("got %v, wanted %v", err.Error(), expectedErrMsg)
	}
}

func TestUpdateJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()
	j.Id = uuid.New()
	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	res, _ := q.RequestJob("email")
	res.Status = StatusCompleted
	res.CompletedAt = time.Now()

	job, err := q.UpdateJob("email", res)

	if job.Id != res.Id {
		t.Errorf("got %v, wanted %v", job.Id, res.Id)
	}

	if job.Status != res.Status {
		t.Errorf("got %v, wanted %v", job.Status, res.Status)
	}

	if job.CompletedAt != res.CompletedAt {
		t.Errorf("got %v, wanted %v", job.CompletedAt, res.CompletedAt)
	}

	if job.Name != j.Name {
		t.Errorf("Name was overwritten: got %v, wanted %v", job.Name, j.Name)
	}

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

}
