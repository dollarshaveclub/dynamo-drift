package jobmanager_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dollarshaveclub/jobmanager"

	"strings"
)

func newTestJob(errors []error) *testJob {
	return &testJob{
		errors: errors,
	}
}

type testJob struct {
	errors  []error
	current int
}

func (t *testJob) next() error {
	err := t.errors[t.current]
	t.current++
	return err
}

func (t *testJob) totalCalls() int {
	return t.current
}

func (t *testJob) getFunc() func(context.Context, ...interface{}) error {
	return func(context.Context, ...interface{}) error {
		return t.next()
	}
}

func TestJobHappyPath(t *testing.T) {
	jobStub := newTestJob([]error{nil})
	job := &jobmanager.Job{
		Job:     jobStub.getFunc(),
		Retries: 2,
	}

	if err := job.ProcessJob(context.TODO()); err != nil {
		t.Fatalf("expected no error: %s", err)
	}

	if jobStub.totalCalls() != 1 {
		t.Fatalf("expected 1 job stub call")
	}
}

func TestErrorRetries(t *testing.T) {
	e := errors.New("test error")
	jobStub := newTestJob([]error{e, e, nil})
	job := &jobmanager.Job{
		Job:             jobStub.getFunc(),
		Retries:         2,
		RetryableErrors: []error{e},
		RetryFunc:       jobmanager.DoNothing,
	}

	if err := job.ProcessJob(context.TODO()); err != nil {
		t.Fatalf("expected %s, got %s", e, err)
	}

	if calls := jobStub.totalCalls(); calls != 3 {
		t.Fatalf("expected 3 job stub calls, got: %d", calls)
	}
}

func TestSubstringRetries(t *testing.T) {
	e := errors.New("test error")
	jobStub := newTestJob([]error{e, e, nil})
	job := &jobmanager.Job{
		Job:                      jobStub.getFunc(),
		Retries:                  2,
		RetryableErrorSubstrings: []string{"test error"},
		RetryFunc:                jobmanager.DoNothing,
	}

	if err := job.ProcessJob(context.TODO()); err != nil {
		t.Fatalf("expected %s, got %s", e, err)
	}

	if calls := jobStub.totalCalls(); calls != 3 {
		t.Fatalf("expected 3 job stub calls, got: %d", calls)
	}
}

func TestPlainRetries(t *testing.T) {
	e := errors.New("test error")
	jobStub := newTestJob([]error{e, e, nil})
	job := &jobmanager.Job{
		Job:       jobStub.getFunc(),
		Retries:   2,
		RetryFunc: jobmanager.DoNothing,
	}

	if err := job.ProcessJob(context.TODO()); err != nil {
		t.Fatalf("expected %s, got %s", e, err)
	}

	if calls := jobStub.totalCalls(); calls != 3 {
		t.Fatalf("expected 3 job stub calls, got: %d", calls)
	}
}

func TestNoRetryMatches(t *testing.T) {
	e := errors.New("test error")
	jobStub := newTestJob([]error{e, e, nil})
	job := &jobmanager.Job{
		Job:                      jobStub.getFunc(),
		Retries:                  2,
		RetryFunc:                jobmanager.DoNothing,
		RetryableErrorSubstrings: []string{"fake test error"},
	}

	if err := job.ProcessJob(context.TODO()); err != e {
		t.Fatalf("expected %s, got %s", e, err)
	}

	if calls := jobStub.totalCalls(); calls != 1 {
		t.Fatalf("expected 1 job stub calls, got: %d", calls)
	}
}

func TestJMRunProcessesAllJobs(t *testing.T) {
	options := func(jm *jobmanager.JobManager) {
		jm.Concurrency = 2
	}
	m := jobmanager.New(options)

	jobStub1 := newTestJob([]error{nil})
	job1 := &jobmanager.Job{
		Job:       jobStub1.getFunc(),
		Retries:   2,
		RetryFunc: jobmanager.DoNothing,
	}
	m.AddJob(job1)

	jobStub2 := newTestJob([]error{nil})
	job2 := &jobmanager.Job{
		Job:       jobStub2.getFunc(),
		Retries:   2,
		RetryFunc: jobmanager.DoNothing,
	}
	m.AddJob(job2)

	jobStub3 := newTestJob([]error{errors.New("test error"), errors.New("test error")})
	job3 := &jobmanager.Job{
		Job:       jobStub3.getFunc(),
		Retries:   1,
		RetryFunc: jobmanager.DoNothing,
	}
	m.AddJob(job3)

	m.Run(context.TODO())

	if err := m.Err(); !strings.Contains(err.Error(), "test error") {
		t.Fatalf("invalid error returned: %s", err)
	}

	if jobStub1.totalCalls() != 1 {
		t.Fatalf("expected 1 job stub call")
	}
	if jobStub2.totalCalls() != 1 {
		t.Fatalf("expected 1 job stub call")
	}
	if jobStub3.totalCalls() != 2 {
		t.Fatalf("expected 2 job stub calls")
	}
}
