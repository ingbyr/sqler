package main

import (
	"context"
	"encoding/csv"
	"sync"
	"sync/atomic"
)

type CsvWriterJob struct {
	file  *csv.Writer
	data  [][]string
	dsKey string
}

type CsvWriter struct {
	c        chan *CsvWriterJob
	ctx      context.Context
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
	hasError atomic.Bool
}

func (fw *CsvWriter) Submit(job *CsvWriterJob) {
	fw.wg.Add(1)
	fw.c <- job
}

func (fw *CsvWriter) WaitForNoRemainJob() {
	fw.wg.Wait()
}

func (fw *CsvWriter) Shutdown(wait bool) {
	if !wait {
		fw.cancel()
		return
	}
	fw.WaitForNoRemainJob()
}

func (fw *CsvWriter) Start() {
	for {
		select {
		case <-fw.ctx.Done():
			return
		case job := <-fw.c:

			for _, row := range job.data {
				job.file.Write(append(row, job.dsKey))
			}
			job.file.Flush()
			fw.wg.Done()
		}
	}

}
