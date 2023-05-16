package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"sqler/pkg"
	"strconv"
	"strings"
	"sync"
)

var _ ExecutableJob = (*SqlJob)(nil)

func NewSqlJob(stmt string, jobId int, totalJobSize int, dsCfg *pkg.DataSourceConfig, db *sql.DB) Job {
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) > %s\n", jobId, totalJobSize, dsCfg.Url, dsCfg.Schema, stmt)
	execWg := &sync.WaitGroup{}
	execWg.Add(1)
	stmt, useVerticalResult := checkStmtOptions(stmt)
	job := &SqlJob{
		Stmt:              stmt,
		DB:                db,
		Prefix:            prefix,
		UseVerticalResult: useVerticalResult,
	}
	return NewJob(Info, job)
}

type SqlJob struct {
	Stmt              string
	DB                *sql.DB
	Prefix            string
	SqlRows           *sql.Rows
	UseVerticalResult bool
	Err               error
	*DefaultJob
}

func (job *SqlJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *SqlJob) DoExec() error {
	job.SqlRows, job.Err = job.DB.Query(job.Stmt)
	return job.Err
}

func (job *SqlJob) Output() []byte {
	job.Wait()

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

	// Some DDL return nothing
	if len(sqlColumns) == 0 && len(sqlResultLines) == 0 {
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
	// Format as lines
	if job.UseVerticalResult {
		maxLen := 0
		for i := range headers {
			if len(headers[i]) > maxLen {
				maxLen = len(headers[i])
			}
		}
		maxLen += 2
		for i := range columns {
			b.WriteString("******************* " + strconv.Itoa(i) + ". rows *******************\n")
			for j := range headers {
				for k := maxLen - len(headers[j]); k >= 0; k-- {
					b.WriteByte(' ')
				}
				b.WriteString(headers[j])
				b.WriteString(": ")
				b.WriteString(columns[i][j])
				b.WriteByte('\n')
			}
		}
		return
	}

	// Format as table
	table := tablewriter.NewWriter(b)
	table.SetHeader(headers)
	for i := range columns {
		table.Append(columns[i])
	}
	table.Render()
}

func checkStmtOptions(stmt string) (string, bool) {
	if strings.HasSuffix(stmt, `\G`) {
		return stmt[:len(stmt)-2], true
	}
	return stmt, false
}
