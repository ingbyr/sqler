package main

import (
	"context"
	"sync"
	"sync/atomic"
)

func NewJobExecutor(jobGroupSize int, printer *CompositedPrinter) *JobExecutor {
	return NewJobExecutorWithCache(jobGroupSize, 1, printer)
}

func NewJobExecutorWithCache(jobGroupSize int, cacheSize int, printer *CompositedPrinter) *JobExecutor {
	if jobGroupSize <= 0 || jobGroupSize > 1024 {
		panic("Job group size must in [1, 1024]")
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	jobGroup := make([]chan Job, jobGroupSize)
	for i := range jobGroup {
		jobGroup[i] = make(chan Job, cacheSize)
	}
	return &JobExecutor{
		jobGroup:   jobGroup,
		ctx:        ctx,
		cancel:     cancelFunc,
		printer:    printer,
		totalJobWg: new(sync.WaitGroup),
	}
}

type JobExecutor struct {
	jobGroup   []chan Job
	ctx        context.Context
	cancel     context.CancelFunc
	printer    *CompositedPrinter
	totalJobWg *sync.WaitGroup
	hasError   atomic.Bool
}

func (e *JobExecutor) Start() {
	for i := 0; i < len(e.jobGroup); i++ {
		go e.handleJob(e.jobGroup[i])
	}
}

func (e *JobExecutor) Submit(job Job, jobGroupId int) {
	jobChan := e.jobGroup[jobGroupId]
	e.totalJobWg.Add(1)
	jobChan <- job
	if job.IsPrintable() {
		e.printer.Print(job)
	}
}

func (e *JobExecutor) WaitForNoRemainJob() {
	e.totalJobWg.Wait()
	e.printer.WaitForNoJob(true)
}

func (e *JobExecutor) Shutdown(wait bool) {
	if !wait {
		e.cancel()
		return
	}
	e.WaitForNoRemainJob()
}

// HasAnyError will be reset to false when invoked
func (e *JobExecutor) HasAnyError() bool {
	defer e.hasError.Store(false)
	return e.hasError.Load()
}

func (e *JobExecutor) handleJob(jobChan chan Job) {
	for {
		select {
		case <-e.ctx.Done():
			return
		case job := <-jobChan:
			err := job.Exec()
			if err != nil {
				job.SetError(err)
				e.hasError.Store(true)
			}
			job.Done()
			e.totalJobWg.Done()
		}
	}
}
