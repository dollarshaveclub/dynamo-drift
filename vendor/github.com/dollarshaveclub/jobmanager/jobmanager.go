package jobmanager

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"os"

	"github.com/hashicorp/go-multierror"
)

// RetryFunc customizes the job behavior that's performed in between
// retries.
type RetryFunc func(context.Context, uint)

// ErrorHandler is an interface that can process errors.
type ErrorHandler interface {
	HandleError(error) error
}

// Job specifies the type of job JobManager can execute. A Job can be
// customized to have retry behavior.
type Job struct {
	Name                     string
	Job                      func(context.Context, ...interface{}) error
	Retries                  uint
	RetryableErrors          []error
	RetryableErrorSubstrings []string
	RetryFunc                RetryFunc
	Logger                   interface {
		Printf(message string, args ...interface{})
	}
	ErrorHandler ErrorHandler
}

// ExponentialBackoff is a RetryFunc that waits an exponentially
// increasing amount of time in between retries.
var ExponentialBackoff RetryFunc = func(ctx context.Context, retryNum uint) {
	select {
	case <-time.After((1 << retryNum) * time.Second):
	case <-ctx.Done():
	}
}

// DoNothingWithErrorsHandler does nothing with error values
type DoNothingWithErrorsHandler struct{}

// HandleError does nothing
func (dnwe DoNothingWithErrorsHandler) HandleError(err error) error {
	return nil
}

// DoNothingWithErrors is the default error handler if none is provided.
var DoNothingWithErrors DoNothingWithErrorsHandler

// DoNothing is a RetryFunc that does nothing in between retries.
var DoNothing RetryFunc = func(_ context.Context, _ uint) {}

func (j *Job) setupJobDefaults() {
	if j == nil {
		return
	}
	if j.Name == "" {
		j.Name = "anonymous-job"
	}
	if j.RetryFunc == nil {
		j.RetryFunc = ExponentialBackoff
	}
	if j.Logger == nil {
		j.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if j.ErrorHandler == nil {
		j.ErrorHandler = DoNothingWithErrors
	}
}

// ProcessJob processes a Job taking into account the retry behavior.
func (j *Job) ProcessJob(ctx context.Context, args ...interface{}) error {
	totalAttempts := j.Retries + 1
	var err error

retryLoop:
	for i := uint(0); i < totalAttempts; i++ {
		// This ensures that we don't retry on the last
		// iteration through the loop.
		tryRetryFunc := func() {
			if i < totalAttempts-1 {
				j.Logger.Printf("job(%s) - retrying job that failed with error: %s", j.Name, err)
				j.RetryFunc(ctx, uint(i))
			}
		}

		// Check if we should cancel execution.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = j.Job(ctx, args...)
		if err == nil {
			return nil
		}
		if err != nil && j.ErrorHandler != nil {
			j.ErrorHandler.HandleError(err)
		}

		if len(j.RetryableErrors) > 0 {
			for _, e := range j.RetryableErrors {
				if e == err {
					tryRetryFunc()
					continue retryLoop
				}
			}
		}

		if len(j.RetryableErrorSubstrings) > 0 {
			for _, ss := range j.RetryableErrorSubstrings {
				if strings.Contains(err.Error(), ss) {
					tryRetryFunc()
					continue retryLoop
				}
			}
		}

		if j.Retries > 0 && len(j.RetryableErrors) == 0 && len(j.RetryableErrorSubstrings) == 0 {
			tryRetryFunc()
			continue retryLoop
		}

		// We didn't match any of the retryable errors or
		// substrings.
		return err
	}

	return err
}

// JobManager handles the concurrent execution of jobs.
type JobManager struct {
	sync.Mutex

	// Options
	Identifier   string
	Concurrency  uint
	Logger       *log.Logger
	ErrorHandler ErrorHandler

	jobs       []*Job
	jobArgs    [][]interface{}
	resultChan chan error
	errors     []error
	started    bool
}

// New creates a new JobManager.
func New(options ...func(*JobManager)) *JobManager {
	jm := &JobManager{}

	// Functional options, inspiration:
	// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
	for _, option := range options {
		option(jm)
	}

	if jm.Logger == nil {
		jm.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if jm.Identifier == "" {
		jm.Identifier = "anonymous-job-manager"
	}

	return jm
}

// Printf logs to the JobManager's logger.
func (m *JobManager) Printf(message string, args ...interface{}) {
	m.Logger.Printf(
		fmt.Sprintf("job-manager(%s) - %s", m.Identifier, message),
		args...,
	)
}

// AddJob adds a Job to the Job manager.
func (m *JobManager) AddJob(j *Job, args ...interface{}) {
	m.Lock()
	defer m.Unlock()

	if j == nil {
		return
	}
	j.setupJobDefaults()
	m.jobs = append(m.jobs, j)
	m.jobArgs = append(m.jobArgs, args)
}

func (m *JobManager) _start(ctx context.Context) {
	m.Lock()
	if m.started {
		m.Unlock()
		return
	}
	m.started = true
	m.Unlock()

	m.Printf("queueing all jobs")

	if m.Concurrency == 0 {
		m.Concurrency = uint(len(m.jobs))
	}

	m.resultChan = make(chan error, len(m.jobs))

	type jobMsg struct {
		job  *Job
		args []interface{}
	}
	msgChan := make(chan *jobMsg)

	for i := uint(0); i < m.Concurrency; i++ {
		go func() {
			for msg := range msgChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				m.resultChan <- msg.job.ProcessJob(ctx, msg.args...)
			}
		}()
	}

	go func() {
		for i, job := range m.jobs {
			select {
			case <-ctx.Done():
				continue
			default:
			}

			msgChan <- &jobMsg{
				job:  job,
				args: m.jobArgs[i],
			}
		}
		close(msgChan)

	}()
}

// Run executes all jobs, waiting for them to complete.
func (m *JobManager) Run(ctx context.Context) {
	m._start(ctx)

	for range m.jobs {
		select {
		case <-ctx.Done():
			m.errors = append(m.errors, ctx.Err())
			return
		case err := <-m.resultChan:
			if err != nil {
				m.errors = append(m.errors, err)
			}
		}
	}

	m.Printf("finished running all jobs")
}

// Err returns the all of the errors resulting from the execution of
// all jobs.
func (m *JobManager) Err() error {
	if len(m.errors) == 0 {
		return nil
	}

	var err error
	for _, e := range m.errors {
		err = multierror.Append(err, e)
	}

	return err
}
