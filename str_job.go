package main

import "sync"

type StrPrintJob struct {
	msg string
	*DefaultPrintJob
}

func NewStrPrintJob(msg string, level Level, printable *sync.WaitGroup, printWg *sync.WaitGroup) *StrPrintJob {
	return &StrPrintJob{
		msg:             msg,
		DefaultPrintJob: NewDefaultPrintJob(level, printable, printWg),
	}
}

func (p *StrPrintJob) Msg() []byte {
	return []byte(p.msg)
}
