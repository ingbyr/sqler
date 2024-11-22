package main

import (
	"context"
	"sync"
	"sync/atomic"
)

func NewJobExecutor(jobGroupSize int) *JobExecutor {
	return NewJobExecutorWithCache(jobGroupSize, 1)
}

func NewJobExecutorWithCache(jobGroupSize int, cacheSize int) *JobExecutor {
	if jobGroupSize <= 0 || jobGroupSize > 1024 {
		panic("Job group size must in [1, 1024]")
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	jobGroup := make([]chan Job, jobGroupSize)
	for i := range jobGroup {
		jobGroup[i] = make(chan Job, cacheSize)
	}
	jobExecutor := &JobExecutor{
		jobChGroup: jobGroup,
		doneJobCh:  make(chan Job, 16),
		ctx:        ctx,
		cancel:     cancelFunc,
		jobWg:      new(sync.WaitGroup),
		doneJobWg:  new(sync.WaitGroup),
	}
	return jobExecutor
}

type JobExecutor struct {
	jobChGroup []chan Job
	doneJobCh  chan Job
	ctx        context.Context
	cancel     context.CancelFunc
	jobWg      *sync.WaitGroup
	doneJobWg  *sync.WaitGroup
	hasError   atomic.Bool
}

func (je *JobExecutor) Start() {
	go je.handleDoneJob()
	for i := 0; i < len(je.jobChGroup); i++ {
		go je.handleJob(je.jobChGroup[i])
	}
}

func (je *JobExecutor) Submit(job Job, jobGroupId int) {
	je.jobWg.Add(1)
	je.jobChGroup[jobGroupId] <- job

	je.doneJobWg.Add(1)
	je.doneJobCh <- job

	job.AfterSubmit()
}

func (je *JobExecutor) WaitForNoRemainJob() {
	je.jobWg.Wait()
	je.doneJobWg.Wait()
}

func (je *JobExecutor) Shutdown(wait bool) {
	if !wait {
		je.cancel()
		return
	}
	je.WaitForNoRemainJob()
}

// HasAnyError will be reset to false when invoked
func (je *JobExecutor) HasAnyError() bool {
	defer je.hasError.Store(false)
	return je.hasError.Load()
}

func (je *JobExecutor) handleJob(jobChan chan Job) {
	for {
		select {
		case <-je.ctx.Done():
			return
		case job := <-jobChan:
			job.BeforeExec()
			for _, msg := range job.BeforeOutput() {
				printer.Info(msg)
			}
			job.Exec()
			if job.Error() != nil {
				je.hasError.Store(true)
			}
			job.AfterExec()
			job.MarkDone()
			je.jobWg.Done()
		}
	}
}

func (je *JobExecutor) handleDoneJob() {
	for {
		select {
		case <-je.ctx.Done():
			return
		case doneJob := <-je.doneJobCh:
			doneJob.Wait()
			doneJob.AfterDone()
			for _, out := range doneJob.DoneOutput() {
				printer.Info(out)
			}
			if doneJob.Error() != nil {
				printer.Error("", doneJob.Error())
			}
			je.doneJobWg.Done()
		}
	}
}
