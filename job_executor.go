package main

import (
	"context"
	"sync"
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
	return &JobExecutor{
		jobGroup:   jobGroup,
		ctx:        ctx,
		cancel:     cancelFunc,
		totalJobWg: new(sync.WaitGroup),
	}
}

type JobExecutor struct {
	jobGroup   []chan Job
	ctx        context.Context
	cancel     context.CancelFunc
	printer    *JobPrinter
	totalJobWg *sync.WaitGroup
}

func (e *JobExecutor) Start() {
	for i := 0; i < len(e.jobGroup); i++ {
		go func(jobChan chan Job) {
			for {
				select {
				case <-e.ctx.Done():
					return
				case job := <-jobChan:
					err := job.Exec()
					if err != nil {
						job.SetError(err)
						if job.PanicWhenError() {
							panic(err)
						}
					}
					job.Done()
					job.DoneGroup()
					e.totalJobWg.Done()
				}
			}
		}(e.jobGroup[i])
	}
}

func (e *JobExecutor) Submit(job Job, jobGroupId int) {
	jobs := e.jobGroup[jobGroupId]
	e.totalJobWg.Add(1)
	jobs <- job
}

func (e *JobExecutor) WaitForNoRemainJob() {
	e.totalJobWg.Wait()
}

func (e *JobExecutor) Shutdown(wait bool) {
	if !wait {
		e.cancel()
		return
	}
	e.WaitForNoRemainJob()
	e.cancel()
}
