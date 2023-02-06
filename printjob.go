package main

import "sync"

type PrintJob interface {
	Name() string
	WaitForPrint()
	Msg() []byte
	Level() MsgLevel
	Printed() *sync.WaitGroup
}

var _ PrintJob = (*DefaultPrintJob)(nil)

type DefaultPrintJob struct {
	name      string
	printable *sync.WaitGroup
	msg       string
	level     MsgLevel
	printed   *sync.WaitGroup
}

func NewDefaultPrintJob(name string, msg string, level MsgLevel, printable *sync.WaitGroup) *DefaultPrintJob {
	printed := &sync.WaitGroup{}
	printed.Add(1)
	return &DefaultPrintJob{
		name:      name,
		printable: printable,
		msg:       msg,
		level:     level,
		printed:   printed,
	}
}

func (p *DefaultPrintJob) Name() string {
	return p.name
}

func (p *DefaultPrintJob) WaitForPrint() {
	if p.printable != nil {
		p.printable.Wait()
	}
}

func (p *DefaultPrintJob) Msg() []byte {
	return []byte(p.msg)
}

func (p *DefaultPrintJob) Level() MsgLevel {
	return p.level
}

func (p *DefaultPrintJob) Printed() *sync.WaitGroup {
	return p.printed
}
