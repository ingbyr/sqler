package main

import (
	"context"
	"database/sql"
	"sqler/pkg"
)

type Sqler struct {
	ctx         context.Context
	cfg         *pkg.Config
	dbs         []*sql.DB
	tableMetas  []*TableMeta
	columnMeats []*ColumnMeta
	jobExecutor *JobExecutor
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
		dbs:         make([]*sql.DB, len(cfg.DataSources)),
		tableMetas:  make([]*TableMeta, 0, 32),
		columnMeats: make([]*ColumnMeta, 0, 128),
		jobExecutor: NewJobExecutor(len(cfg.DataSources)),
	}

	// Init db and stmt job chan
	s.ConnectToDb()

	// Start sql job
	s.jobExecutor.Start()
	return s
}

func (s *Sqler) ConnectToDb() {
	jobExecutor := NewJobExecutor(len(s.dbs))
	jobExecutor.Start()
	for idx := 0; idx < len(s.dbs); idx++ {
		connJob := NewConnJob(s, idx)
		jobExecutor.Submit(connJob, idx)
	}
	jobExecutor.Shutdown(true)
}

// ExecSerial executes sql in turn (each sql and database)
func (s *Sqler) ExecSerial(jobCtx *JobCtx, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 0
	for _, stmt := range stmts {
		jobCtx.CsvFileHeaderWrote = false
		for dbId := range s.dbs {
			jobId++
			job := NewSqlJob(stmt, jobId, jobSize, s.cfg.DataSources[dbId], s.dbs[dbId], jobCtx)
			s.jobExecutor.Submit(job, dbId)
			s.jobExecutor.WaitForNoRemainJob()
		}
	}
}

// ExecPara executes sql in parallel (each database)
func (s *Sqler) ExecPara(jobCtx *JobCtx, stmts ...string) {
	jobSize := s.totalStmtSize(len(stmts))
	jobId := 0
	for _, stmt := range stmts {
		jobCtx.CsvFileHeaderWrote = false
		for dbId := range s.dbs {
			jobId++
			job := NewSqlJob(stmt, jobId, jobSize, s.cfg.DataSources[dbId], s.dbs[dbId], jobCtx)
			s.jobExecutor.Submit(job, dbId)
		}
		s.jobExecutor.WaitForNoRemainJob()
		if jobCtx.StopWhenError && s.jobExecutor.HasAnyError() {
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
