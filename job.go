package main

import (
	"bytes"
	"sync"
)

type Job interface {
	Exec() error
	Wait()
	MarkDone()
	SetError(err error)
	StopOtherJobsWhenError() bool
	AfterSubmit()
	BeforeExec()
	AfterExec()
	AfterDone()
	PrintNow(msg string)
	PrintAfterDone(msg string)
}

func NewBaseJob(ctx *SqlJobCtx) *BaseJob {
	b := &BaseJob{
		ctx:    ctx,
		result: new(bytes.Buffer),
		wg:     new(sync.WaitGroup),
		err:    nil,
	}
	b.wg.Add(1)
	return b
}

var _ Job = (*BaseJob)(nil)

type BaseJob struct {
	ctx    *SqlJobCtx
	result *bytes.Buffer
	wg     *sync.WaitGroup
	err    error
}

func (b *BaseJob) PrintNow(msg string) {
	b.ctx.Printer.Info(msg)
}

func (b *BaseJob) PrintAfterDone(msg string) {
	b.result.WriteString(msg)
}

func (b *BaseJob) AfterSubmit() {
}

func (b *BaseJob) BeforeExec() {
}

func (b *BaseJob) AfterExec() {
}

func (b *BaseJob) AfterDone() {
	b.ctx.Printer.Info(b.result.String())
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
