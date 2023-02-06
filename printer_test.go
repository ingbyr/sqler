package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPrinter_PrintJob(t *testing.T) {
	p := NewPrinter()
	pwg := &sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		pwg.Add(1)
		p.Print(NewStrPrintJob(fmt.Sprintf("%d\n", i), Debug, nil, pwg))
	}

	pwg.Add(1)
	go func() {
		printable := &sync.WaitGroup{}
		printable.Add(1)
		p.Print(NewStrPrintJob("5\n", Debug, printable, pwg))
		time.Sleep(1 * time.Second)
		printable.Done()
	}()

	pwg.Add(1)
	p.Print(NewStrPrintJob("4\n", Debug, nil, pwg))

	pwg.Wait()
}
