package main

import (
	"fmt"
	"os"
)

func NewPrinter() *Printer {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	checkError("can not open output.log", err)
	return &Printer{
		outputFile: outputFile,
	}
}

type Printer struct {
	outputFile *os.File
}

func (p *Printer) WriteString(s string) (n int, err error) {
	fmt.Print(s)
	return p.outputFile.WriteString(s)
}

func (p *Printer) Write(b []byte) (n int, err error) {
	_, _ = os.Stdout.Write(b)
	return p.outputFile.Write(b)
}
