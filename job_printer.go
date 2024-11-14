package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	PrintJobCacheSize = 32
)

type CompositedPrinter struct {
	f    *os.File
	jobs chan Job
	wg   *sync.WaitGroup
}

func NewJobPrinter() *CompositedPrinter {
	err := os.Mkdir("log", os.ModePerm)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
	logFilePath := fmt.Sprintf("log/%d.log", time.Now().Unix())
	outputFile, err := os.OpenFile(logFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &CompositedPrinter{
		f:    outputFile,
		jobs: make(chan Job, PrintJobCacheSize),
		wg:   new(sync.WaitGroup),
	}
	go p.Execute()
	return p
}

func (printer *CompositedPrinter) WaitForNoJob() {
	printer.wg.Wait()
}

func (printer *CompositedPrinter) Print(job Job) {
	printer.wg.Add(1)
	printer.jobs <- job
}

func (printer *CompositedPrinter) PrintInfo(msg string) {
	printer.Print(NewStrJob(msg, Info))
}

func (printer *CompositedPrinter) LogInfo(msg string) {
	job := NewStrJob(msg, Info)
	job.SetPrintable(false)
	printer.Print(job)
}

func (printer *CompositedPrinter) PrintError(msg string, err error) {
	printer.Print(NewStrJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error))
}

func (printer *CompositedPrinter) Execute() {
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

func (printer *CompositedPrinter) writeString(s string, toStdout bool) {
	if toStdout {
		printer.writeStringToStdout(s)
	}
	printer.writeStringToFile(s)
}

func (printer *CompositedPrinter) writeBytes(b []byte, toStdOut bool) {
	if toStdOut {
		printer.writeBytesToStdout(b)
	}
	printer.writeBytesToFile(b)
}

func (printer *CompositedPrinter) writeStringToStdout(s string) {
	n, err := os.Stdout.WriteString(s)
	mustNoIoError(n, err)
}

func (printer *CompositedPrinter) writeBytesToStdout(b []byte) {
	n, err := os.Stdout.Write(b)
	mustNoIoError(n, err)
}

func (printer *CompositedPrinter) writeStringToFile(s string) {
	n, err := printer.f.WriteString(s)
	mustNoIoError(n, err)
}

func (printer *CompositedPrinter) writeBytesToFile(b []byte) {
	n, err := printer.f.Write(b)
	mustNoIoError(n, err)
}

func mustNoIoError(n int, err error) {
	if err != nil {
		err := fmt.Errorf("n %d, err %v", n, err)
		panic(err)
	}
}
