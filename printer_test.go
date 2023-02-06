package main

import (
	"fmt"
	"testing"
)

func TestPrinter_PrintJob(t *testing.T) {
	p := NewPrinter()
	for i := 0; i < 3; i++ {
		p.Print(NewDefaultPrintJob("test", fmt.Sprintf("msg%d\n", i), MsgDebug, nil))
	}
}
