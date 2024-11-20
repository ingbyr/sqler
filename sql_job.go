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
	DsCfg             *pkg.DataSourceConfig
	Prefix            string
	SqlRows           *sql.Rows
	UseVerticalResult bool
	*SqlJobCtx
	*DefaultJob
}

func NewSqlJob(stmt string, jobId int, totalJobSize int, dsCfg *pkg.DataSourceConfig, db *sql.DB, opts *SqlJobCtx) Job {
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) > %s\n", jobId, totalJobSize, dsCfg.Url, dsCfg.Schema, stmt)
	stmt, useVerticalResult := parseStmt(stmt)
	job := &SqlJob{
		Stmt:              stmt,
		DB:                db,
		DsCfg:             dsCfg,
		Prefix:            prefix,
		UseVerticalResult: useVerticalResult,
		SqlJobCtx:         opts,
	}
	return WrapJob(job)
}

func (job *SqlJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *SqlJob) DoExec() error {
	job.output.WriteString(job.Prefix)
	var err error
	job.SqlRows, err = job.DB.Query(job.Stmt)
	if err != nil {
		return err
	}
	// Convert sql rows to string array
	sqlColumns, sqlResultLines, err := convertSqlResults(job.SqlRows)
	if err != nil {
		return err
	}

	// Export data to csv if necessary
	if job.ExportCsv {
		job.exportDataToCsv(sqlColumns, sqlResultLines)
		job.printable = false
		return nil
	}

	// Some DDL return nothing
	if len(sqlColumns) == 0 && len(sqlResultLines) == 0 {
		job.output.Write([]byte("OK\n"))
	}

	// Format sql results
	if len(sqlColumns) != 0 && len(sqlResultLines) != 0 {
		job.writeWithFormat(job.output, sqlColumns, sqlResultLines)
	}

	return nil
}

func (job *SqlJob) MsgError(err error, b *bytes.Buffer) []byte {
	job.level = Error
	b.WriteString(err.Error())
	b.WriteByte('\n')
	return b.Bytes()
}

func (job *SqlJob) writeWithFormat(b *bytes.Buffer, headers []string, columns [][]string) {
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

func (job *SqlJob) exportDataToCsv(headers []string, rows [][]string) {
	job.CsvFileLock.Lock()
	defer job.CsvFileLock.Unlock()
	dsKey := job.DsCfg.DsKey()
	comPrinter.PrintInfo(fmt.Sprintf("[%s] Exporting data to %s ...", dsKey, job.CsvFileName))
	if !job.CsvFileHeaderWrote {
		job.CsvFile.Write(append(headers, "Data Source"))
		job.CsvFileHeaderWrote = true
	}
	for _, row := range rows {
		job.CsvFile.Write(append(row, job.DsCfg.DsKey()))
	}
	job.CsvFile.Flush()
	comPrinter.PrintInfo(fmt.Sprintf("[%s] Done", dsKey))
}

func parseStmt(stmt string) (string, bool) {
	if strings.HasSuffix(stmt, `\G`) {
		return stmt[:len(stmt)-2], true
	}
	return stmt, false
}
