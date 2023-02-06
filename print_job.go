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
}

var _ PrintJob = (*DefaultPrintJob)(nil)
var _ PrintJob = (*StrPrintJob)(nil)

type DefaultPrintJob struct {
	level     Level
	printable *sync.WaitGroup
	printed   *sync.WaitGroup
	printWg   *sync.WaitGroup
}

func NewDefaultPrintJob(level Level, printable *sync.WaitGroup, printWg *sync.WaitGroup) *DefaultPrintJob {
	printed := &sync.WaitGroup{}
	printed.Add(1)
	return &DefaultPrintJob{
		level:     level,
		printable: printable,
		printed:   printed,
		printWg:   printWg,
	}
}

func (p *DefaultPrintJob) Msg() []byte {
	panic("implement me")
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
