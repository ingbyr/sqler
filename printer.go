package main

import (
	"fmt"
	"os"
	"time"
)

const (
	PrintJobCacheSize = 32
)

type Printer struct {
	outputFile *os.File
	jobs       chan PrintJob
}

func NewPrinter() *Printer {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &Printer{
		outputFile: outputFile,
		jobs:       make(chan PrintJob, PrintJobCacheSize),
	}
	go p.doPrint()
	return p
}

func (p *Printer) WriteString(s string) (n int, err error) {
	fmt.Print(s)
	return p.outputFile.WriteString(s)
}

func (p *Printer) Write(b []byte) (n int, err error) {
	_, _ = os.Stdout.Write(b)
	return p.outputFile.Write(b)
}

func (p *Printer) WaitForPrinted() {
	for {
		if len(p.jobs) > 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
	}
}

func (p *Printer) Print(job PrintJob) {
	p.jobs <- job
}

func (p *Printer) PrintInfo(msg string) {
	p.Print(NewStrPrintJob(msg, Info, nil, nil))
}

func (p *Printer) PrintError(msg string, err error) {
	p.Print(NewStrPrintJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error, nil, nil))
}

func (p *Printer) doPrint() {
	for {
		select {
		case job := <-p.jobs:
			job.WaitForPrint()
			if job.Level() != Info {
				p.WriteString(job.Level().String())
			}
			p.Write(job.Msg())
			p.Write([]byte("\n"))
			job.Printed().Done()
			if job.PrintWg() != nil {
				job.PrintWg().Done()
			}
			if job.ErrorQuit() && job.Level() == Error {
				os.Exit(1)
			}
		}
	}
}
