package main

import (
	"database/sql"
	"fmt"
	"sqler/pkg"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

type ConnJob struct {
	dbId  int
	sqler *Sqler
	*BaseJob
}

func NewConnJob(sqler *Sqler, dbId int) Job {
	return &ConnJob{
		dbId:    dbId,
		sqler:   sqler,
		BaseJob: NewBaseJob(new(JobCtx)),
	}
}

func (job *ConnJob) StopOtherJobsWhenError() bool {
	return true
}

func (job *ConnJob) Exec() {
	ds := job.sqler.cfg.DataSources[job.dbId]
	dsArgs := job.sqler.cfg.DataSourceArgs
	db, err := job.connect(ds, dsArgs)
	if job.RecordError(err) {
		return
	}

	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	job.sqler.dbs[job.dbId] = db
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
		job.Print(fmt.Sprintf("Failed to parse dsn, %v", err))
		return nil, err
	}
	if err = db.PingContext(job.sqler.ctx); err != nil {
		job.Print(fmt.Sprintf("Failed to connect db, %v", err))
		return nil, err
	}
	job.Print(fmt.Sprintf("[%d/%d] Connected %s", job.dbId+1, len(job.sqler.dbs),
		fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, "******", ds.Url, ds.Schema, dsArgs)))
	return db, nil
}

func (job *ConnJob) connectSqlLite(ds *pkg.DataSourceConfig, args string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+ds.Schema+".sqlite")
	job.Print(fmt.Sprintf("[%d/%d] Connected %s", job.dbId+1, len(job.sqler.dbs),
		fmt.Sprintf("%s:%s@tcp(%s)/%s", ds.Username, "******", ds.Url, ds.Schema)))
	return db, err
}
