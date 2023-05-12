package main

import (
	"database/sql"
	"fmt"
	"sync"
)

var _ ExecutableJob = (*ConnJob)(nil)

type ConnJob struct {
	Idx   int
	sqler *Sqler
	*DefaultJob
}

func NewConnJob(idx int, doneGroup *sync.WaitGroup, sqler *Sqler) Job {
	connJob := &ConnJob{
		Idx:   idx,
		sqler: sqler,
	}
	return NewJobGroup(Info, doneGroup, connJob)
}

func (job *ConnJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *ConnJob) QuitWhenError() bool {
	return true
}

func (job *ConnJob) DoExec() error {
	ds := job.sqler.cfg.DataSources[job.Idx]
	dsArgs := job.sqler.cfg.DataSourceArgs
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, dsArgs)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		job.output.WriteString(fmt.Sprintf("Failed to parse dsn, %v", err))
		job.level = Error
		return err
	}
	if err = db.PingContext(job.sqler.ctx); err != nil {
		job.output.WriteString(fmt.Sprintf("Failed to connect db, %v", err))
		job.level = Error
		return err
	}
	job.output.WriteString(fmt.Sprintf("[%d/%d] Connected %s", job.Idx+1, job.sqler.dbSize, dsn))
	job.sqler.dbs[job.Idx] = db
	job.sqler.sqlJobs[job.Idx] = make(chan *SqlJob, SqlJobCacheSize)
	return nil
}
