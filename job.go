package main

import (
	"bytes"
	"sync"
)

type Job interface {
	AfterDoneOutput() string
	Exec() error
	Wait()
	MarkDone()
	StopOtherJobsWhenError() bool
	AfterSubmit()
	BeforeExec()
	AfterExec(err error)
	AfterDone()
	PrintNow(msg string)
	PrintAfterDone(msg string)
}

func NewBaseJob(ctx *JobCtx) *BaseJob {
	b := &BaseJob{
		ctx:        ctx,
		doneOutput: new(bytes.Buffer),
		wg:         new(sync.WaitGroup),
		err:        nil,
	}
	b.wg.Add(1)
	return b
}

var _ Job = (*BaseJob)(nil)

type BaseJob struct {
	ctx        *JobCtx
	doneOutput *bytes.Buffer
	wg         *sync.WaitGroup
	err        error
}

func (b *BaseJob) AfterDoneOutput() string {
	return b.doneOutput.String()
}

func (b *BaseJob) PrintNow(msg string) {
	printer.Info(msg)
}

func (b *BaseJob) PrintAfterDone(msg string) {
	b.doneOutput.WriteString(msg)
}

func (b *BaseJob) AfterSubmit() {
}

func (b *BaseJob) BeforeExec() {
}

func (b *BaseJob) AfterExec(err error) {
}

func (b *BaseJob) AfterDone() {
}

func (b *BaseJob) Exec() error {
	panic("implement me")
}

func (b *BaseJob) Wait() {
	b.wg.Wait()
}

func (b *BaseJob) MarkDone() {
	b.wg.Done()
}

func (b *BaseJob) StopOtherJobsWhenError() bool {
	return false
}

func (b *BaseJob) SetError(err error) {
	b.err = err
}

func (b *BaseJob) Error() error {
	return b.err
}
