package main

import (
	"bytes"
	"database/sql"
	"github.com/olekukonko/tablewriter"
	"strconv"
	"sync"
)

var _ PrintJob = (*SqlJob)(nil)

type SqlJob struct {
	Stmt              string
	ExecWg            *sync.WaitGroup
	Db                *sql.DB
	Prefix            string
	SqlRows           *sql.Rows
	UseVerticalResult bool
	Err               error
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
	job.format(b, sqlColumns, sqlResultLines)

	return b.Bytes()
}

func (job *SqlJob) MsgError(err error, b *bytes.Buffer) []byte {
	job.level = Error
	b.WriteString(err.Error())
	b.WriteByte('\n')
	return b.Bytes()
}

func (job *SqlJob) format(b *bytes.Buffer, headers []string, columns [][]string) {
	if job.UseVerticalResult {
		for i := range columns {
			b.WriteString("************** " + strconv.Itoa(i) + ". rows **************\n")
			for j := range headers {
				b.WriteString(headers[j])
				b.WriteString(": ")
				b.WriteString(columns[i][j])
				b.WriteByte('\n')
			}
		}
		return
	}
	table := tablewriter.NewWriter(b)
	table.SetHeader(headers)
	for i := range columns {
		table.Append(columns[i])
	}
	table.Render()
}
