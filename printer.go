package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type printerMsg struct {
	msg        []byte
	isStdOut   bool
	isLoggable bool
}

type CompositedPrinter struct {
	f  *os.File
	mu sync.Mutex
}

func NewPrinter() *CompositedPrinter {
	err := os.Mkdir("log", os.ModePerm)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
	logFilePath := fmt.Sprintf("log/%d.log", time.Now().Unix())
	outputFile, err := os.OpenFile(logFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &CompositedPrinter{
		f:  outputFile,
		mu: sync.Mutex{},
	}
	return p
}

func (p *CompositedPrinter) Info(msg string) {
	p.print(&printerMsg{
		msg:        append([]byte(msg), '\n'),
		isStdOut:   true,
		isLoggable: true,
	})
}

func (p *CompositedPrinter) Log(msg string) {
	p.print(&printerMsg{
		msg:        append([]byte(msg), '\n'),
		isStdOut:   false,
		isLoggable: true,
	})
}

func (p *CompositedPrinter) Error(msg string, err error) {
	p.print(&printerMsg{
		msg:        []byte(fmt.Sprintf("[Error] %s: %s\n", msg, err.Error())),
		isStdOut:   true,
		isLoggable: true,
	})
}

func (p *CompositedPrinter) print(msg *printerMsg) {
	if msg.isStdOut {
		p.writeBytesToStdout(msg.msg)
	}
	if msg.isLoggable {
		p.writeBytesToFile(msg.msg)
	}
}

func (p *CompositedPrinter) writeBytesToStdout(b []byte) {
	n, err := os.Stdout.Write(b)
	mustNoIoError(n, err)
}

func (p *CompositedPrinter) writeBytesToFile(b []byte) {
	n, err := p.f.Write(b)
	mustNoIoError(n, err)
}

func mustNoIoError(n int, err error) {
	if err != nil {
		err := fmt.Errorf("n %d, err %v", n, err)
		panic(err)
	}
}
