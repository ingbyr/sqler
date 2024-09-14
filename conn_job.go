package main

import (
	"database/sql"
	"fmt"
	"sqler/pkg"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

var _ ExecutableJob = (*ConnJob)(nil)

type ConnJob struct {
	Idx   int
	sqler *Sqler
	*DefaultJob
}

func NewConnJob(idx int, sqler *Sqler) Job {
	connJob := &ConnJob{
		Idx:   idx,
		sqler: sqler,
	}
	return WrapJob(connJob)
}

func (job *ConnJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *ConnJob) StopOtherJobsWhenError() bool {
	return true
}

func (job *ConnJob) DoExec() error {
	ds := job.sqler.cfg.DataSources[job.Idx]
	dsArgs := job.sqler.cfg.DataSourceArgs
	db, err := job.connect(ds, dsArgs)
	if err != nil {
		return err
	}
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	job.sqler.dbs[job.Idx] = db
	return nil
}

func (job *ConnJob) connect(ds *pkg.DataSourceConfig, dsArgs string) (*sql.DB, error) {
	switch ds.Type {
	case "mysql":
		return job.connectMySQL(ds, dsArgs)
	case "sqlite3":
		return job.connectSqlLite(ds, dsArgs)
	}
	panic("Not supported database type: " + ds.Type)
}

func (job *ConnJob) connectMySQL(ds *pkg.DataSourceConfig, dsArgs string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, dsArgs)
	db, err := sql.Open(ds.Type, dsn)
	if err != nil {
		job.output.WriteString(fmt.Sprintf("Failed to parse dsn, %v", err))
		job.level = Error
		return nil, err
	}
	if err = db.PingContext(job.sqler.ctx); err != nil {
		job.output.WriteString(fmt.Sprintf("Failed to connect db, %v", err))
		job.level = Error
		return nil, err
	}
	job.output.WriteString(fmt.Sprintf("[%d/%d] Connected %s", job.Idx+1, len(job.sqler.dbs),
		fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, "******", ds.Url, ds.Schema, dsArgs)))
	return db, nil
}

func (job *ConnJob) connectSqlLite(ds *pkg.DataSourceConfig, args string) (*sql.DB, error) {
	return sql.Open("sqlite3", "file:"+ds.Schema+".sqlite")
}
