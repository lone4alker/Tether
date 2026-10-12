package metrics

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type Metrics struct {
	submitted atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	pending   atomic.Int64
	leased    atomic.Int64
}

type Snapshot struct {
	Submitted int64
	Completed int64
	Failed    int64
	Pending   int64
	Leased    int64
}

func (m *Metrics) JobSubmitted() {
	m.submitted.Add(1)
	m.pending.Add(1)
}

func (m *Metrics) JobCompleted() {
	m.leased.Add(-1)
	m.completed.Add(1)
}

func (m *Metrics) JobLeased() {
	m.pending.Add(-1)
	m.leased.Add(1)
}

func (m *Metrics) JobFailed() {
	m.leased.Add(-1)
	m.failed.Add(1)
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Submitted: m.submitted.Load(),
		Completed: m.completed.Load(),
		Failed:    m.failed.Load(),
		Pending:   m.pending.Load(),
		Leased:    m.leased.Load(),
	}
}

func (s Snapshot) Prometheus() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# HELP tether_jobs_submitted_total Total jobs durably accepted into the broker.\n")
	fmt.Fprintf(&b, "# TYPE tether_jobs_submitted_total counter\n")
	fmt.Fprintf(&b, "tether_jobs_submitted_total %d\n", s.Submitted)

	fmt.Fprintf(&b, "# HELP tether_jobs_completed_total Total jobs completed.\n")
	fmt.Fprintf(&b, "# TYPE tether_jobs_completed_total counter\n")
	fmt.Fprintf(&b, "tether_jobs_completed_total %d\n", s.Completed)

	fmt.Fprintf(&b, "# HELP tether_jobs_failed_total Total jobs failed.\n")
	fmt.Fprintf(&b, "# TYPE tether_jobs_failed_total counter\n")
	fmt.Fprintf(&b, "tether_jobs_failed_total %d\n", s.Failed)

	fmt.Fprintf(&b, "# HELP tether_jobs_pending Jobs currently waiting to be leased.\n")
	fmt.Fprintf(&b, "# TYPE tether_jobs_pending gauge\n")
	fmt.Fprintf(&b, "tether_jobs_pending %d\n", s.Pending)

	fmt.Fprintf(&b, "# HELP tether_jobs_leased Jobs currently leased.\n")
	fmt.Fprintf(&b, "# TYPE tether_jobs_leased gauge\n")
	fmt.Fprintf(&b, "tether_jobs_leased %d\n", s.Leased)

	return b.String()
}