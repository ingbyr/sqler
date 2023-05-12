package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPrinter_PrintJob(t *testing.T) {
	p := NewJobExecutor()
	pwg := &sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		pwg.Add(1)
		p.Print(NewPrintJob(fmt.Sprintf("%d\n", i), Debug).SetPrintWg(pwg))
	}

	pwg.Add(1)
	go func() {
		printable := &sync.WaitGroup{}
		printable.Add(1)
		p.Print(NewPrintJob("5\n", Debug).SetPrintable(printable).SetPrintWg(pwg))
		time.Sleep(1 * time.Second)
		printable.Done()
	}()

	pwg.Add(1)
	p.Print(NewPrintJob("4\n", Debug).SetPrintWg(pwg))

	pwg.Wait()
}
