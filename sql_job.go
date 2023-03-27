package main

import (
	"bytes"
	"database/sql"
	"github.com/olekukonko/tablewriter"
	"sync"
)

var _ PrintJob = (*SqlJob)(nil)

type SqlJob struct {
	Stmt    string
	ExecWg  *sync.WaitGroup
	Db      *sql.DB
	Prefix  string
	SqlRows *sql.Rows
	Err     error
	*DefaultPrintJob
}

func (job *SqlJob) Msg() []byte {
	job.ExecWg.Wait()
	// Convert sql rows to string array
	b := new(bytes.Buffer)
	b.WriteString(job.Prefix)
	if job.Err != nil {
		return job.MsgError(job.Err, b)
	}
	sqlColumns, sqlResultLines, err := convertSqlResults(job.SqlRows)
	if err != nil {
		return job.MsgError(err, b)
	}
	// Convert to table format
	if len(sqlColumns) == 0 && len(sqlResultLines) == 0 {
		// Some DDL return nothing
		b.Write([]byte("OK"))
		return b.Bytes()
	}
	table := tablewriter.NewWriter(b)
	table.SetHeader(sqlColumns)
	for j := range sqlResultLines {
		table.Append(sqlResultLines[j])
	}
	table.Render()
	return b.Bytes()
}

func (job *SqlJob) MsgError(err error, b *bytes.Buffer) []byte {
	job.level = Error
	b.WriteString(err.Error())
	b.WriteByte('\n')
	return b.Bytes()
}
