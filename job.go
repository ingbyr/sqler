package main

import (
	"bytes"
	"sync"
)

func NewJob(level Level, job ExecutableJob) *DefaultJob {
	done := new(sync.WaitGroup)
	done.Add(1)
	defaultJob := &DefaultJob{
		job:       job,
		level:     level,
		output:    new(bytes.Buffer),
		done:      done,
		doneGroup: nil,
		printable: true,
	}
	job.SetWrapper(defaultJob)
	return defaultJob
}

func NewJobGroup(level Level, doneGroup *sync.WaitGroup, job ExecutableJob) *DefaultJob {
	defaultJob := NewJob(level, job)
	defaultJob.SetDoneGroup(doneGroup)
	return defaultJob
}

type ExecutableJob interface {
	DoExec() error
	Output() []byte
	SetWrapper(job *DefaultJob)
}

type Job interface {
	Output() []byte
	MustExec()
	Level() Level
	Wait()
	QuitWhenError() bool
	WaitDone()
	SetDone(done *sync.WaitGroup)
	WaitDoneGroup()
	SetDoneGroup(doneGroup *sync.WaitGroup)
	IsPrintable() bool
	SetPrintable(printable bool)
}

var _ Job = (*DefaultJob)(nil)
var _ Job = (*PrintJob)(nil)

type DefaultJob struct {
	job       ExecutableJob
	level     Level
	output    *bytes.Buffer
	done      *sync.WaitGroup
	doneGroup *sync.WaitGroup
	printable bool
}

func (d *DefaultJob) Exec() error {
	err := d.job.DoExec()
	if d.done != nil {
		d.done.Done()
	}
	if d.doneGroup != nil {
		d.doneGroup.Done()
	}
	return err
}

func (d *DefaultJob) MustExec() {
	if err := d.Exec(); err != nil {
		panic(err)
	}
}

func (d *DefaultJob) WaitDone() {
	if d.done != nil {
		d.done.Wait()
	}
}

func (d *DefaultJob) SetDone(done *sync.WaitGroup) {
	d.done = done
}

func (d *DefaultJob) WaitDoneGroup() {
	if d.doneGroup != nil {
		d.doneGroup.Done()
	}
}

func (d *DefaultJob) SetDoneGroup(doneGroup *sync.WaitGroup) {
	d.doneGroup = doneGroup
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

func (d *DefaultJob) Level() Level {
	return d.level
}

func (d *DefaultJob) QuitWhenError() bool {
	return false
}
