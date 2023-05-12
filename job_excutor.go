package main

import (
	"fmt"
	"os"
	"time"
)

const (
	PrintJobCacheSize = 32
)

type JobExecutor struct {
	outputFile *os.File
	jobs       chan Job
}

func NewJobExecutor() *JobExecutor {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &JobExecutor{
		outputFile: outputFile,
		jobs:       make(chan Job, PrintJobCacheSize),
	}
	go p.Execute()
	return p
}

func (p *JobExecutor) WriteString(s string) (n int, err error) {
	return os.Stdout.WriteString(s)
}

func (p *JobExecutor) Write(b []byte) (n int, err error) {
	return os.Stdout.Write(b)
}

func (p *JobExecutor) SaveString(s string) (int, error) {
	return p.outputFile.WriteString(s)
}

func (p *JobExecutor) SaveBytes(b []byte) (int, error) {
	return p.outputFile.Write(b)
}

func (p *JobExecutor) WaitForPrinted() {
	for {
		if len(p.jobs) > 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
	}
}

func (p *JobExecutor) Print(job Job) {
	p.jobs <- job
}

func (p *JobExecutor) PrintInfo(msg string) {
	p.Print(NewPrintJob(msg, Info))
}

func (p *JobExecutor) LogInfo(msg string) {
	p.Print(NewPrintJob(msg, Info))
}

func (p *JobExecutor) PrintError(msg string, err error) {
	p.Print(NewPrintJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error))
}

func (p *JobExecutor) Execute() {
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
			if job.QuitWhenError() {
				os.Exit(1)
			}
		}
	}
}
