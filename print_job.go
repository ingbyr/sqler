package main

import (
	"sync"
)

type PrintJob interface {
	Msg() []byte
	Level() Level
	Printed() *sync.WaitGroup
	PrintWg() *sync.WaitGroup
	WaitForPrint()
	ErrorQuit() bool
	SetPrintable(printable *sync.WaitGroup) PrintJob
	SetPrinted(printed *sync.WaitGroup) PrintJob
	SetPrintWg(printWg *sync.WaitGroup) PrintJob
	SetVisible(visible bool) PrintJob
	Visible() bool
}

var _ PrintJob = (*DefaultPrintJob)(nil)
var _ PrintJob = (*StrPrintJob)(nil)

type DefaultPrintJob struct {
	level     Level
	printable *sync.WaitGroup
	printed   *sync.WaitGroup
	printWg   *sync.WaitGroup
	printJob  PrintJob
	visible   bool
}

func NewDefaultPrintJob(level Level) *DefaultPrintJob {
	printed := &sync.WaitGroup{}
	printed.Add(1)
	return &DefaultPrintJob{
		level:     level,
		printable: nil,
		printed:   printed,
		printWg:   nil,
		visible:   true,
	}
}

func (p *DefaultPrintJob) Msg() []byte {
	return p.printJob.Msg()
}

func (p *DefaultPrintJob) PrintWg() *sync.WaitGroup {
	return p.printWg
}

func (p *DefaultPrintJob) WaitForPrint() {
	if p.printable != nil {
		p.printable.Wait()
	}
}

func (p *DefaultPrintJob) Level() Level {
	return p.level
}

func (p *DefaultPrintJob) Printed() *sync.WaitGroup {
	return p.printed
}

func (p *DefaultPrintJob) ErrorQuit() bool {
	return false
}

func (p *DefaultPrintJob) SetPrintable(printable *sync.WaitGroup) PrintJob {
	p.printable = printable
	return p
}

func (p *DefaultPrintJob) SetPrinted(printed *sync.WaitGroup) PrintJob {
	p.printed = printed
	return p
}

func (p *DefaultPrintJob) SetPrintWg(printWg *sync.WaitGroup) PrintJob {
	p.printWg = printWg
	return p
}

func (p *DefaultPrintJob) SetVisible(visible bool) PrintJob {
	p.visible = visible
	return p
}

func (p *DefaultPrintJob) Visible() bool {
	return p.visible
}
