package main

import (
	"errors"
	"sync"
)

type Job interface {
	DoneOutput() []string
	BeforeOutput() []string
	Exec()
	Wait()
	MarkDone()
	StopOtherJobsWhenError() bool
	AfterSubmit()
	BeforeExec()
	AfterExec()
	AfterDone()
	PrintBeforeExec(msg string)
	PrintAfterDone(msg string)
	Error() error
	RecordError(err error) bool
}

func NewBaseJob(ctx *JobCtx) *BaseJob {
	b := &BaseJob{
		ctx:          ctx,
		beforeOutput: make([]string, 0),
		doneOutput:   make([]string, 0),
		wg:           new(sync.WaitGroup),
		err:          nil,
	}
	b.wg.Add(1)
	return b
}

var _ Job = (*BaseJob)(nil)

type BaseJob struct {
	ctx          *JobCtx
	beforeOutput []string
	doneOutput   []string
	wg           *sync.WaitGroup
	err          error
}

func (b *BaseJob) BeforeOutput() []string {
	return b.beforeOutput
}

func (b *BaseJob) PrintBeforeExec(msg string) {
	b.beforeOutput = append(b.beforeOutput, msg)
}

func (b *BaseJob) DoneOutput() []string {
	return b.doneOutput
}

func (b *BaseJob) PrintAfterDone(msg string) {
	b.doneOutput = append(b.doneOutput, msg)
}

func (b *BaseJob) AfterSubmit() {
}

func (b *BaseJob) BeforeExec() {
}

func (b *BaseJob) AfterExec() {
}

func (b *BaseJob) AfterDone() {
}

func (b *BaseJob) Exec() {
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

func (b *BaseJob) Error() error {
	return b.err
}

func (b *BaseJob) RecordError(err error) bool {
	if err != nil {
		b.err = errors.Join(b.err, err)
		return true
	}
	return false
}
