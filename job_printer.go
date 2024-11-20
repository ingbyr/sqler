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

type CompositedPrinter struct {
	f  *os.File
	ch chan []byte
	wg *sync.WaitGroup
}

func NewJobPrinter() *CompositedPrinter {
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
		ch: make(chan []byte, PrintJobCacheSize),
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
	p.ch <- []byte(msg)
}

func (p *CompositedPrinter) ByteInfo(msg []byte) {
	p.wg.Add(1)
	p.ch <- msg
}

func (p *CompositedPrinter) Error(msg string, err error) {
	p.wg.Add(1)
	p.ch <- []byte(fmt.Sprintf("[Error] %s: %s", msg, err.Error()))
}

func (p *CompositedPrinter) Execute() {
	for {
		select {
		case msg := <-p.ch:
			p.writeBytes(msg)
			// Mark print job jobWg
			p.wg.Done()
		}
	}
}

func (p *CompositedPrinter) writeBytes(b []byte) {
	p.writeBytesToStdout(b)
	p.writeBytesToFile(b)
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
