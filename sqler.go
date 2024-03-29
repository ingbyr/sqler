package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"sqler/pkg"
	"sync"
)

type Sqler struct {
	ctx         context.Context
	cfg         *pkg.Config
	dbs         []*sql.DB
	tableMetas  []*TableMeta
	columnMeats []*ColumnMeta
	jobExecutor *JobExecutor
	jobPrinter  *JobPrinter
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

func NewSqler(cfg *pkg.Config, printer *JobPrinter) *Sqler {
	s := &Sqler{
		ctx:         context.Background(),
		cfg:         cfg,
		dbs:         make([]*sql.DB, len(cfg.DataSources)),
		tableMetas:  make([]*TableMeta, 0, 32),
		columnMeats: make([]*ColumnMeta, 0, 128),
		jobExecutor: NewJobExecutor(len(cfg.DataSources), printer),
	}

	// Init db and stmt job chan
	jobExecutor := NewJobExecutor(len(s.dbs), printer)
	jobExecutor.Start()
	for dbID := 0; dbID < len(s.dbs); dbID++ {
		connJob := NewConnJob(dbID, s)
		jobExecutor.Submit(connJob, dbID)
	}
	jobExecutor.Shutdown(true)

	// Start sql job
	s.jobExecutor.Start()
	return s
}

// ExecSync executes sql in turn (each sql and database)
func (s *Sqler) ExecSync(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 0
	batchWg := &sync.WaitGroup{}
	for _, stmt := range stmts {
		for dbId := range s.dbs {
			jobId++
			batchWg.Add(1)
			job := NewSqlJob(stmt, jobId, jobSize, s.cfg.DataSources[dbId], s.dbs[dbId])
			s.jobExecutor.Submit(job, dbId)
			s.jobExecutor.WaitForNoRemainJob()
		}
	}
	batchWg.Wait()
}

// ExecPara executes sql in parallel (each database)
func (s *Sqler) ExecPara(stopWhenError bool, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 0
	for _, stmt := range stmts {
		for dbId := range s.dbs {
			jobId++
			job := NewSqlJob(stmt, jobId, jobSize, s.cfg.DataSources[dbId], s.dbs[dbId])
			s.jobExecutor.Submit(job, dbId)
		}
		s.jobExecutor.WaitForNoRemainJob()
		if stopWhenError && s.jobExecutor.HasAnyError() {
			return
		}
	}
}

func (s *Sqler) totalStmtSize(stmtSize int) int {
	return len(s.dbs) * stmtSize
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
