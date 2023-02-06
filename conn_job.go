package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
)

var _ PrintJob = (*ConnJob)(nil)

type ConnJob struct {
	Idx    int
	ExecWg *sync.WaitGroup
	Result *bytes.Buffer
	*DefaultPrintJob
}

func NewConnJob(idx int, printWg *sync.WaitGroup) *ConnJob {
	execWg := new(sync.WaitGroup)
	execWg.Add(1)
	return &ConnJob{
		Idx:             idx,
		ExecWg:          execWg,
		Result:          new(bytes.Buffer),
		DefaultPrintJob: NewDefaultPrintJob(Info, execWg, printWg),
	}
}

func (job *ConnJob) Msg() []byte {
	job.ExecWg.Wait()
	return job.Result.Bytes()
}

func (job *ConnJob) ErrorQuit() bool {
	return true
}

func (job *ConnJob) Exec(s *Sqler) {
	defer job.ExecWg.Done()
	ds := s.cfg.DataSources[job.Idx]
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, s.cfg.DataSourceArg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		job.Result.WriteString(fmt.Sprintf("Failed to parse dsn, %v", err))
		job.level = Error
		return
	}
	if err = db.PingContext(s.ctx); err != nil {
		job.Result.WriteString(fmt.Sprintf("Failed to connect db, %v", err))
		job.level = Error
		return
	}
	job.Result.WriteString(fmt.Sprintf("[%d/%d] Connected %s", job.Idx+1, s.dbSize, dsn))
	s.dbs[job.Idx] = db
	s.sqlJobs[job.Idx] = make(chan *SqlJob, SqlJobCacheSize)
	return
}
