package main

import "sync"

type PrintJob interface {
	Msg() []byte
	Level() MsgLevel
	Printed() *sync.WaitGroup
	PrintWg() *sync.WaitGroup
	WaitForPrint()
}

var _ PrintJob = (*StrPrintJob)(nil)

type DefaultPrintJob struct {
	level     MsgLevel
	printable *sync.WaitGroup
	printed   *sync.WaitGroup
	printWg   *sync.WaitGroup
}

func NewDefaultPrintJob(level MsgLevel, printable *sync.WaitGroup, printWg *sync.WaitGroup) *DefaultPrintJob {
	printed := &sync.WaitGroup{}
	printed.Add(1)
	return &DefaultPrintJob{
		level:     level,
		printable: printable,
		printed:   printed,
		printWg:   printWg,
	}
}

func (p *DefaultPrintJob) PrintWg() *sync.WaitGroup {
	return p.printWg
}

func (p *DefaultPrintJob) WaitForPrint() {
	if p.printable != nil {
		p.printable.Wait()
	}
}

func (p *DefaultPrintJob) Level() MsgLevel {
	return p.level
}

func (p *DefaultPrintJob) Printed() *sync.WaitGroup {
	return p.printed
}

type StrPrintJob struct {
	msg string
	*DefaultPrintJob
}

func NewStrPrintJob(msg string, level MsgLevel, printable *sync.WaitGroup, printWg *sync.WaitGroup) *StrPrintJob {
	return &StrPrintJob{
		msg:             msg,
		DefaultPrintJob: NewDefaultPrintJob(level, printable, printWg),
	}
}

func (p *StrPrintJob) Msg() []byte {
	return []byte(p.msg)
}
