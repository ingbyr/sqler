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
	return os.Stdout.WriteString(s)
}

func (p *Printer) Write(b []byte) (n int, err error) {
	return os.Stdout.Write(b)
}

func (p *Printer) SaveString(s string) (int, error) {
	return p.outputFile.WriteString(s)
}

func (p *Printer) SaveBytes(b []byte) (int, error) {
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
	p.Print(NewStrPrintJob(msg, Info))
}

func (p *Printer) LogInfo(msg string) {
	p.Print(NewStrPrintJob(msg, Info).SetVisible(false))
}

func (p *Printer) PrintError(msg string, err error) {
	p.Print(NewStrPrintJob(fmt.Sprintf("%s: %s", msg, err.Error()), Error))
}

func (p *Printer) doPrint() {
	for {
		select {
		case job := <-p.jobs:
			job.WaitForPrint()
			if job.Level() != Info {
				levelString := job.Level().String()
				if job.Visible() {
					p.WriteString(levelString)
				}
				p.SaveString(levelString)
			}
			msg := job.Msg()
			if job.Visible() {
				p.Write(msg)
			}
			p.SaveBytes(msg)
			if job.Visible() {
				p.Write([]byte("\n"))
			}
			p.SaveBytes([]byte("\n"))
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
