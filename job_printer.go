package main

import (
	"fmt"
	"os"
)

const (
	PrintJobCacheSize = 32
)

type JobPrinter struct {
	outputFile *os.File
	jobs       chan Job
}

func NewJobPrinter() *JobPrinter {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &JobPrinter{
		outputFile: outputFile,
		jobs:       make(chan Job, PrintJobCacheSize),
	}
	go p.Execute()
	return p
}

func (p *JobPrinter) WriteString(s string) (n int, err error) {
	return os.Stdout.WriteString(s)
}

func (p *JobPrinter) Write(b []byte) (n int, err error) {
	return os.Stdout.Write(b)
}

func (p *JobPrinter) SaveString(s string) (int, error) {
	return p.outputFile.WriteString(s)
}

func (p *JobPrinter) SaveBytes(b []byte) (int, error) {
	return p.outputFile.Write(b)
}

func (p *JobPrinter) WaitForPrinted() {
}

func (p *JobPrinter) Print(job Job) {
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
			job.Wait()
			levelString := job.Level().String()
			if job.IsPrintable() {
				p.WriteString(levelString)
			}
			p.SaveString(levelString)
			msg := job.Output()
			if job.IsPrintable() {
				p.Write(msg)
			}
			p.SaveBytes(msg)
			if job.IsPrintable() {
				p.Write([]byte("\n"))
			}
			p.SaveBytes([]byte("\n"))
			if job.PanicWhenError() {
				os.Exit(1)
			}
		}
	}
}
