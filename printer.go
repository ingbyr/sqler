package main

import (
	"fmt"
	"os"
	"time"
)

type MsgLevel = uint

const (
	PrintJobCacheSize = 32
	MsgDebug          = iota
	MsgInfo
	MsgWarn
	MsgError
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

func (p *Printer) doPrint() {
	for {
		select {
		case job := <-p.jobs:
			job.WaitForPrint()
			p.Write(job.Msg())
			job.Printed().Done()
			if job.PrintWg() != nil {
				job.PrintWg().Done()
			}
		}
	}
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

func (p *Printer) CheckError(msg string, err error) {
	if err != nil {
		p.PrintError(msg, err)
		os.Exit(1)
	}
}

func (p *Printer) PrintError(msg string, err error) {
	p.WriteString("\n======= ERROR ========\n")
	p.WriteString(fmt.Sprintf("message: %s\n", msg))
	p.WriteString(fmt.Sprintf("error  : %v\n", err))
}
