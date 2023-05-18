package main

import (
	"fmt"
	"os"
	"sync"
)

const (
	PrintJobCacheSize = 32
)

type JobPrinter struct {
	f    *os.File
	jobs chan Job
	wg   *sync.WaitGroup
}

func NewJobPrinter() *JobPrinter {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &JobPrinter{
		f:    outputFile,
		jobs: make(chan Job, PrintJobCacheSize),
		wg:   new(sync.WaitGroup),
	}
	go p.Execute()
	return p
}

func (printer *JobPrinter) WaitForNoJob() {
	printer.wg.Wait()
}

func (printer *JobPrinter) Print(job Job) {
	printer.wg.Add(1)
	printer.jobs <- job
}

func (printer *JobPrinter) PrintInfo(msg string) {
	printer.Print(NewStrJob(msg, Info))
}

func (printer *JobPrinter) LogInfo(msg string) {
	job := NewStrJob(msg, Info)
	job.SetPrintable(false)
	printer.Print(job)
}

func (printer *JobPrinter) PrintError(msg string, err error) {
	printer.Print(NewStrJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error))
}

func (printer *JobPrinter) Execute() {
	for {
		select {
		case job := <-printer.jobs:
			// Wait for done
			job.Wait()
			msg := job.Output()
			err := job.Error()
			level := job.Level()
			toStdOut := job.IsPrintable()
			if err != nil {
				level = Error
			}
			if level != Info && len(msg) != 0 {
				printer.writeString(level.String(), toStdOut)
				printer.writeBytes([]byte(" "), toStdOut)
				printer.writeBytes(msg, toStdOut)
			} else {
				printer.writeBytes(msg, toStdOut)
			}
			if err != nil {
				printer.writeString(Error.String(), true)
				printer.writeBytes([]byte(" "), toStdOut)
				printer.writeString(err.Error(), true)
				printer.writeBytes([]byte("\n"), true)
			}
			printer.writeBytes([]byte("\n"), toStdOut)
			// Mark print job done
			printer.wg.Done()
		}
	}
}

func (printer *JobPrinter) writeString(s string, toStdout bool) {
	if toStdout {
		printer.writeStringToStdout(s)
	}
	printer.writeStringToFile(s)
}

func (printer *JobPrinter) writeBytes(b []byte, toStdOut bool) {
	if toStdOut {
		printer.writeBytesToStdout(b)
	}
	printer.writeBytesToFile(b)
}

func (printer *JobPrinter) writeStringToStdout(s string) {
	n, err := os.Stdout.WriteString(s)
	mustNoIoError(n, err)
}

func (printer *JobPrinter) writeBytesToStdout(b []byte) {
	n, err := os.Stdout.Write(b)
	mustNoIoError(n, err)
}

func (printer *JobPrinter) writeStringToFile(s string) {
	n, err := printer.f.WriteString(s)
	mustNoIoError(n, err)
}

func (printer *JobPrinter) writeBytesToFile(b []byte) {
	n, err := printer.f.Write(b)
	mustNoIoError(n, err)
}

func mustNoIoError(n int, err error) {
	if err != nil {
		err := fmt.Errorf("n %d, err %v", n, err)
		panic(err)
	}
}
