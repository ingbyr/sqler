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

func (p *JobPrinter) WaitForNoJob() {
	p.wg.Wait()
}

func (p *JobPrinter) Print(job Job) {
	p.wg.Add(1)
	p.jobs <- job
}

func (p *JobPrinter) PrintInfo(msg string) {
	p.Print(NewPrintJob(msg, Info))
}

func (p *JobPrinter) LogInfo(msg string) {
	p.Print(NewPrintJob(msg, Info))
}

func (p *JobPrinter) PrintError(msg string, err error) {
	p.Print(NewPrintJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error))
}

func (p *JobPrinter) Execute() {
	for {
		select {
		case job := <-p.jobs:
			if !job.IsPrintable() {
				p.wg.Done()
				continue
			}
			job.Wait()
			levelString := job.Level().String()
			p.writeStringToStdout(levelString)
			p.writeStringToFile(levelString)
			msg := job.Output()
			p.writeBytesToStdout(msg)
			p.writeBytesToFile(msg)
			p.writeBytesToStdout([]byte("\n"))
			p.writeBytesToFile([]byte("\n"))
			p.wg.Done()
		}
	}
}

func (p *JobPrinter) writeStringToStdout(s string) {
	n, err := os.Stdout.WriteString(s)
	mustNoIoError(n, err)
}

func (p *JobPrinter) writeBytesToStdout(b []byte) {
	n, err := os.Stdout.Write(b)
	mustNoIoError(n, err)
}

func (p *JobPrinter) writeStringToFile(s string) {
	n, err := p.f.WriteString(s)
	mustNoIoError(n, err)
}

func (p *JobPrinter) writeBytesToFile(b []byte) {
	n, err := p.f.Write(b)
	mustNoIoError(n, err)
}

func mustNoIoError(n int, err error) {
	if err != nil {
		err := fmt.Errorf("n %d, err %v", n, err)
		panic(err)
	}
}
