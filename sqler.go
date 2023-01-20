package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

type Sqler struct {
	ctx     context.Context
	cfg     *Config
	printer *Printer
	dbSize  int
	dbs     []*sql.DB
	sjs     []chan *Job
}

func NewSqler(cfg *Config) *Sqler {
	s := &Sqler{
		ctx:     context.Background(),
		cfg:     cfg,
		printer: NewPrinter(),
		dbSize:  len(cfg.DataSources),
		dbs:     make([]*sql.DB, len(cfg.DataSources)),
		sjs:     make([]chan *Job, len(cfg.DataSources)),
	}
	// Init db and stmt job chan
	for i, ds := range s.cfg.DataSources {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, cfg.DataSourceArg)
		s.printer.WriteString(fmt.Sprintf("dsn: %s\n", dsn))
		db, err := sql.Open("mysql", dsn)
		checkError("failed to connect to the db", err)
		if err = db.Ping(); err != nil {
			panic(err)
		}
		s.dbs[i] = db
		s.sjs[i] = make(chan *Job, jobCacheSize)
	}
	// Listen stmt job chan
	for _, sc := range s.sjs {
		go func(stmtJobs chan *Job) {
			for {
				select {
				case <-quit:
					return
				case job := <-stmtJobs:
					job.Result, job.Err = job.Db.Query(job.Stmt)
					job.Executed.Done()
				}
			}
		}(sc)
	}
	return s
}

// ExecSync executes sql in turn (each sql and database)
func (s *Sqler) ExecSync(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printedWg := &sync.WaitGroup{}
	printedWg.Add(1)
	for _, stmt := range stmts {
		jobs := make([]*Job, s.dbSize)
		for dbId := range s.dbs {
			printedWg.Add(1)
			job := s.SendJob(stmt, dbId, jobId, jobSize, printedWg)
			jobs[dbId] = job
			jobId++
			s.waitForExecuted(job)
		}
		if stopWhenError && s.shouldStop(jobs) {
			break
		}
	}
	printedWg.Done()
	printedWg.Wait()
}

// ExecPara executes sql in parallel (each database)
func (s *Sqler) ExecPara(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printedWg := &sync.WaitGroup{}
	printedWg.Add(1)
	for _, stmt := range stmts {
		jobs := make([]*Job, s.dbSize)
		for dbId := range s.dbs {
			printedWg.Add(1)
			jobs[dbId] = s.SendJob(stmt, dbId, jobId, jobSize, printedWg)
			jobId++
		}
		s.waitForExecuted(jobs...)
		if stopWhenError && s.shouldStop(jobs) {
			break
		}
	}
	printedWg.Done()
	printedWg.Wait()
}

func (s *Sqler) ExecPara0(stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 1
	printedWg := &sync.WaitGroup{}
	printedWg.Add(1)
	for _, stmt := range stmts {
		jobs := make([]*Job, s.dbSize)
		for dbId := range s.dbs {
			printedWg.Add(1)
			jobs[dbId] = s.SendJob(stmt, dbId, jobId, jobSize, printedWg)
			jobId++
		}
	}
	printedWg.Done()
	printedWg.Wait()
}
func (s *Sqler) totalStmtSize(stmtSize int) int {
	return s.dbSize * stmtSize
}

func (s *Sqler) SendJob(stmt string, dbId int, jobId int, totalJobSize int, printedWg *sync.WaitGroup) *Job {
	ds := s.cfg.DataSources[dbId]
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) >", jobId, totalJobSize, ds.Url, ds.Schema)
	executed := &sync.WaitGroup{}
	executed.Add(1)
	job := &Job{
		Stmt:     stmt,
		Db:       s.dbs[dbId],
		Prefix:   prefix,
		Executed: executed,
		Printed:  printedWg,
	}
	s.sjs[dbId] <- job
	s.printer.PrintJob(job)
	return job
}

func (s *Sqler) shouldStop(jobs []*Job) bool {
	for _, job := range jobs {
		if job.Err != nil {
			return true
		}
	}
	return false
}

func (s *Sqler) waitForExecuted(jobs ...*Job) {
	for _, job := range jobs {
		job.Executed.Wait()
	}
}
