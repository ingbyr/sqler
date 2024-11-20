package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	PrintJobCacheSize = 32
)

type printerMsg struct {
	msg        []byte
	isStdOut   bool
	isLoggable bool
}

type CompositedPrinter struct {
	f  *os.File
	ch chan *printerMsg
	wg *sync.WaitGroup
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
		ch: make(chan *printerMsg, PrintJobCacheSize),
		wg: new(sync.WaitGroup),
	}
	go p.Execute()
	return p
}

func (p *CompositedPrinter) Wait() {
	p.wg.Wait()
}

func (p *CompositedPrinter) Info(msg string) {
	p.wg.Add(1)
	p.ch <- &printerMsg{
		msg:        append([]byte(msg), '\n'),
		isStdOut:   true,
		isLoggable: true,
	}
	p.Wait()
}

func (p *CompositedPrinter) Log(msg string) {
	p.wg.Add(1)
	p.ch <- &printerMsg{
		msg:        append([]byte(msg), '\n'),
		isStdOut:   false,
		isLoggable: true,
	}
	p.Wait()
}

func (p *CompositedPrinter) Error(msg string, err error) {
	p.wg.Add(1)
	p.ch <- &printerMsg{
		msg:        []byte(fmt.Sprintf("[Error] %s: %s\n", msg, err.Error())),
		isStdOut:   true,
		isLoggable: true,
	}
	p.Wait()
}

func (p *CompositedPrinter) Execute() {
	for {
		select {
		case msg := <-p.ch:
			if msg.isStdOut {
				p.writeBytesToStdout(msg.msg)
			}
			if msg.isLoggable {
				p.writeBytesToFile(msg.msg)
			}
			p.wg.Done()
		}
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
