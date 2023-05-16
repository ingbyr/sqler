package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"sqler/pkg"
	"strconv"
	"strings"
)

var _ ExecutableJob = (*SqlJob)(nil)

type SqlJob struct {
	Stmt              string
	DB                *sql.DB
	Prefix            string
	SqlRows           *sql.Rows
	UseVerticalResult bool
	Err               error
	*DefaultJob
}

func NewSqlJob(stmt string, jobId int, totalJobSize int, dsCfg *pkg.DataSourceConfig, db *sql.DB) Job {
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) > %s\n", jobId, totalJobSize, dsCfg.Url, dsCfg.Schema, stmt)
	stmt, useVerticalResult := checkStmtOptions(stmt)
	job := &SqlJob{
		Stmt:              stmt,
		DB:                db,
		Prefix:            prefix,
		UseVerticalResult: useVerticalResult,
	}
	return WrapJob(Info, job)
}

func (job *SqlJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *SqlJob) DoExec() error {
	job.output.WriteString(job.Prefix)
	job.SqlRows, job.Err = job.DB.Query(job.Stmt)
	if job.Err != nil {
		return job.Err
	}
	// Convert sql rows to string array
	sqlColumns, sqlResultLines, err := convertSqlResults(job.SqlRows)
	if err != nil {
		return err
	}
	// Some DDL return nothing
	if len(sqlColumns) == 0 && len(sqlResultLines) == 0 {
		job.output.Write([]byte("OK"))
	}
	// Format sql results
	job.format(job.output, sqlColumns, sqlResultLines)
	return nil
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
