package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sqler/pkg"
	"strings"
	"sync"
)

const (
	SqlJobCacheSize = 16
)

type Sqler struct {
	ctx         context.Context
	cfg         *pkg.Config
	dbSize      int
	dbs         []*sql.DB
	sqlJobs     []chan *SqlJob
	tableMetas  []*TableMeta
	columnMeats []*ColumnMeta
}

type TableMeta struct {
	Name    string
	Comment string
}

type ColumnMeta struct {
	Name    string
	Comment string
	Type    string
}

func NewSqler(cfg *pkg.Config) *Sqler {
	s := &Sqler{
		ctx:         context.Background(),
		cfg:         cfg,
		dbSize:      len(cfg.DataSources),
		dbs:         make([]*sql.DB, len(cfg.DataSources)),
		sqlJobs:     make([]chan *SqlJob, len(cfg.DataSources)),
		tableMetas:  make([]*TableMeta, 0, 32),
		columnMeats: make([]*ColumnMeta, 0, 128),
	}

	// Init db and stmt job chan
	doneGroup := &sync.WaitGroup{}
	doneGroup.Add(s.dbSize)
	for i := 0; i < s.dbSize; i++ {
		connJob := NewConnJob(i, doneGroup, s)
		go func() {
			connJob.MustExec()
		}()
		jobExecutor.Print(connJob)
	}
	doneGroup.Wait()

	// Listen stmt job chan
	for _, sc := range s.sqlJobs {
		go func(stmtJobs chan *SqlJob) {
			for {
				select {
				case <-quit:
					return
				case job := <-stmtJobs:
					job.Exec()
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
	stmt, useVerticalResult := s.checkStmtOptions(stmt)
	job := &SqlJob{
		Stmt:              stmt,
		Db:                s.dbs[dbId],
		Prefix:            prefix,
		UseVerticalResult: useVerticalResult,
	}
	NewJob(Info, job)
	s.sqlJobs[dbId] <- job
	// Send print job
	jobExecutor.Print(job)
	return job
}

func (s *Sqler) checkStmtOptions(stmt string) (string, bool) {
	if strings.HasSuffix(stmt, `\G`) {
		return stmt[:len(stmt)-2], true
	}
	return stmt, false
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
		job.WaitDone()
	}
}

func (s *Sqler) loadSchema() error {
	db0 := s.dbs[0]
	schema := s.cfg.DataSources[0].Schema

	tx, err := db0.Begin()
	if err != nil {
		return err
	}
	qtm, err := tx.Prepare(stmtQueryTableMetas)
	if err != nil {
		return err
	}
	rows, err := qtm.Query(schema)
	if err != nil {
		return err
	}
	for rows.Next() {
		tm := &TableMeta{}
		if err := rows.Scan(&tm.Name, &tm.Comment); err != nil {
			return err
		}
		s.tableMetas = append(s.tableMetas, tm)
	}
	defer qtm.Close()

	tx, err = db0.Begin()
	if err != nil {
		return err
	}
	qcm, err := tx.Prepare(stmtQueryColumnMetas)
	if err != nil {
		return err
	}
	rows, err = qcm.Query(schema)
	if err != nil {
		return err
	}
	for rows.Next() {
		cm := &ColumnMeta{}
		if err := rows.Scan(&cm.Name, &cm.Comment, &cm.Type); err != nil {
			return err
		}
		s.columnMeats = append(s.columnMeats, cm)
	}
	defer qcm.Close()

	return nil
}
