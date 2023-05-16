package main

import (
	"bytes"
	"sync"
)

func WrapJob(level Level, job ExecutableJob) *DefaultJob {
	done := new(sync.WaitGroup)
	done.Add(1)
	defaultJob := &DefaultJob{
		job:       job,
		level:     level,
		output:    new(bytes.Buffer),
		done:      done,
		printable: true,
	}
	job.SetWrapper(defaultJob)
	return defaultJob
}

type ExecutableJob interface {
	DoExec() error
	Output() []byte
	SetWrapper(job *DefaultJob)
}

type Job interface {
	Output() []byte
	Exec() error
	Level() Level
	Wait()
	Done()
	IsPrintable() bool
	SetPrintable(printable bool)
	SetError(err error)
	Error() error
	PanicWhenError() bool
}

var _ Job = (*DefaultJob)(nil)
var _ Job = (*StrJob)(nil)

type DefaultJob struct {
	job       ExecutableJob
	level     Level
	output    *bytes.Buffer
	done      *sync.WaitGroup
	printable bool
	err       error
}

func (d *DefaultJob) Exec() error {
	return d.job.DoExec()
}

func (d *DefaultJob) SetDone(done *sync.WaitGroup) {
	d.done = done
}

func (d *DefaultJob) IsPrintable() bool {
	return d.printable
}

func (d *DefaultJob) SetPrintable(printable bool) {
	d.printable = printable
}

func (d *DefaultJob) Output() []byte {
	return d.output.Bytes()
}

func (d *DefaultJob) Wait() {
	if d.done != nil {
		d.done.Wait()
	}
}

func (d *DefaultJob) Done() {
	if d.done != nil {
		d.done.Done()
	}
}

func (d *DefaultJob) Level() Level {
	return d.level
}

func (d *DefaultJob) PanicWhenError() bool {
	return false
}

func (d *DefaultJob) SetError(err error) {
	d.err = err
}

func (d *DefaultJob) Error() error {
	return d.err
}
