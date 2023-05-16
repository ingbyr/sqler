package main

import (
	"context"
	"sync"
)

func NewJobExecutor(jobGroupSize int, printer *JobPrinter) *JobExecutor {
	return NewJobExecutorWithCache(jobGroupSize, 1, printer)
}

func NewJobExecutorWithCache(jobGroupSize int, cacheSize int, printer *JobPrinter) *JobExecutor {
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
	printer    *JobPrinter
	totalJobWg *sync.WaitGroup
}

func (e *JobExecutor) Start() {
	for i := 0; i < len(e.jobGroup); i++ {
		go handleJob(e.ctx, e.jobGroup[i], e.totalJobWg)
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
	e.printer.WaitForNoJob()
}

func (e *JobExecutor) Shutdown(wait bool) {
	if !wait {
		e.cancel()
		return
	}
	e.WaitForNoRemainJob()
}

func handleJob(ctx context.Context, jobChan chan Job, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
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
			wg.Done()
		}
	}
}
