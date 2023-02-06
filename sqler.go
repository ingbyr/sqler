package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

const (
	SqlJobCacheSize = 16
)

type Sqler struct {
	ctx     context.Context
	cfg     *Config
	dbSize  int
	dbs     []*sql.DB
	sqlJobs []chan *SqlJob
}

func NewSqler(cfg *Config) *Sqler {
	s := &Sqler{
		ctx:     context.Background(),
		cfg:     cfg,
		dbSize:  len(cfg.DataSources),
		dbs:     make([]*sql.DB, len(cfg.DataSources)),
		sqlJobs: make([]chan *SqlJob, len(cfg.DataSources)),
	}

	// Init db and stmt job chan
	connCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	initWg := &sync.WaitGroup{}
	initWg.Add(s.dbSize)
	for i := 0; i < s.dbSize; i++ {
		s.initConn(connCtx, i, initWg)
	}
	initWg.Wait()

	// Listen stmt job chan
	for _, sc := range s.sqlJobs {
		go func(stmtJobs chan *SqlJob) {
			for {
				select {
				case <-quit:
					return
				case job := <-stmtJobs:
					job.SqlRows, job.Err = job.Db.Query(job.Stmt)
					job.printable.Done()
				}
			}
		}(sc)
	}

	return s
}

func (s *Sqler) initConn(ctx context.Context, dbIdx int, initialized *sync.WaitGroup) {
	ds := s.cfg.DataSources[dbIdx]
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, s.cfg.DataSourceArg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		printer.PrintError("failed to parse dsn", err)
	}
	if err = db.PingContext(ctx); err != nil {
		printer.PrintError("failed to connect db", err)
	}
	printer.PrintInfo(fmt.Sprintf("[%d/%d] connected %s", dbIdx+1, s.dbSize, dsn))
	s.dbs[dbIdx] = db
	s.sqlJobs[dbIdx] = make(chan *SqlJob, SqlJobCacheSize)
	initialized.Done()
}

// ExecSync executes sql in turn (each sql and database)
func (s *Sqler) ExecSync(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printWg := &sync.WaitGroup{}
	for _, stmt := range stmts {
		jobs := make([]*SqlJob, s.dbSize)
		for dbId := range s.dbs {
			printWg.Add(1)
			job := s.Exec(stmt, dbId, jobId, jobSize, printWg)
			jobs[dbId] = job
			jobId++
			s.waitForExecuted(job)
		}
		if stopWhenError && s.shouldStop(jobs) {
			break
		}
	}
	printWg.Wait()
}

// ExecPara executes sql in parallel (each database)
func (s *Sqler) ExecPara(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printWg := &sync.WaitGroup{}
	for _, stmt := range stmts {
		jobs := make([]*SqlJob, s.dbSize)
		for dbId := range s.dbs {
			printWg.Add(1)
			jobs[dbId] = s.Exec(stmt, dbId, jobId, jobSize, printWg)
			jobId++
		}
		s.waitForExecuted(jobs...)
		if stopWhenError && s.shouldStop(jobs) {
			break
		}
	}
	printWg.Wait()
}

func (s *Sqler) ExecPara0(stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printWg := &sync.WaitGroup{}
	for _, stmt := range stmts {
		jobs := make([]*SqlJob, s.dbSize)
		for dbId := range s.dbs {
			printWg.Add(1)
			jobs[dbId] = s.Exec(stmt, dbId, jobId, jobSize, printWg)
			jobId++
		}
	}
	printWg.Wait()
}
func (s *Sqler) totalStmtSize(stmtSize int) int {
	return s.dbSize * stmtSize
}

func (s *Sqler) Exec(stmt string, dbId int, jobId int, totalJobSize int, printWg *sync.WaitGroup) *SqlJob {
	ds := s.cfg.DataSources[dbId]
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) > %s\n", jobId, totalJobSize, ds.Url, ds.Schema, stmt)
	execWg := &sync.WaitGroup{}
	execWg.Add(1)
	job := &SqlJob{
		Stmt:            stmt,
		ExecWg:          execWg,
		Db:              s.dbs[dbId],
		Prefix:          prefix,
		DefaultPrintJob: NewDefaultPrintJob(Info, execWg, printWg),
	}
	s.sqlJobs[dbId] <- job
	// Send print job
	printer.Print(job)
	return job
}

func (s *Sqler) shouldStop(jobs []*SqlJob) bool {
	for _, job := range jobs {
		if job.Err != nil {
			return true
		}
	}
	return false
}

func (s *Sqler) waitForExecuted(jobs ...*SqlJob) {
	for _, job := range jobs {
		job.ExecWg.Wait()
	}
}
