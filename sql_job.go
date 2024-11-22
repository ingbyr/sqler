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

var _ Job = (*SqlJob)(nil)

type SqlJob struct {
	Stmt              string
	DB                *sql.DB
	DsCfg             *pkg.DataSourceConfig
	Prefix            string
	SqlRows           *sql.Rows
	UseVerticalResult bool
	ctx               *JobCtx
	*BaseJob
}

func NewSqlJob(stmt string, jobId int, totalJobSize int, dsCfg *pkg.DataSourceConfig, db *sql.DB, jobCtx *JobCtx) Job {
	prefix := fmt.Sprintf("[%d/%d] (%s/%s) > %s", jobId, totalJobSize, dsCfg.Url, dsCfg.Schema, stmt)
	stmt, useVerticalResult := parseStmt(stmt)
	return &SqlJob{
		Stmt:              stmt,
		DB:                db,
		DsCfg:             dsCfg,
		Prefix:            prefix,
		UseVerticalResult: useVerticalResult,
		ctx:               jobCtx,
		BaseJob:           NewBaseJob(new(JobCtx)),
	}
}

func (job *SqlJob) BeforeExec() {
	if job.ctx.ExportCsv {
		job.Print(fmt.Sprintf("[%s] Exporting data to %s ...", job.DsCfg.DsKey(), job.ctx.CsvFileName))
	} else {
		job.Print(job.Prefix)
	}
}

func (job *SqlJob) Exec() {
	var err error
	job.SqlRows, err = job.DB.Query(job.Stmt)
	//time.Sleep(time.Duration(3+rand.Intn(8)) * time.Second)
	if job.RecordError(err) {
		return
	}

	// Convert sql rows to string array
	sqlColumns, sqlResultLines, err := convertSqlResults(job.SqlRows)
	if job.RecordError(err) {
		return
	}

	// Export data to csv if necessary
	if job.ctx.ExportCsv {
		job.exportDataToCsv(sqlColumns, sqlResultLines)
		job.Print(fmt.Sprintf("[%s] Exported data to %s", job.DsCfg.DsKey(), job.ctx.CsvFileName))
		return
	}

	// Some DDL return nothing
	if len(sqlColumns) == 0 && len(sqlResultLines) == 0 {
		job.Print(" OK")
	}

	// Format sql results
	if len(sqlColumns) != 0 && len(sqlResultLines) != 0 {
		job.Print(job.formatSqlResult(sqlColumns, sqlResultLines))
	}
}

func (job *SqlJob) formatSqlResult(headers []string, columns [][]string) string {
	b := new(bytes.Buffer)

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
		return b.String()
	}

	// Format as table
	table := tablewriter.NewWriter(b)
	table.SetHeader(headers)
	for i := range columns {
		table.Append(columns[i])
	}
	table.Render()
	return b.String()
}

func (job *SqlJob) exportDataToCsv(headers []string, rows [][]string) {
	job.ctx.CsvFileLock.Lock()
	defer job.ctx.CsvFileLock.Unlock()
	if !job.ctx.CsvFileHeaderWrote {
		job.ctx.CsvFile.Write(append(headers, "Data Source"))
		job.ctx.CsvFileHeaderWrote = true
	}
	for _, row := range rows {
		job.ctx.CsvFile.Write(append(row, job.DsCfg.DsKey()))
	}
	job.ctx.CsvFile.Flush()
}

func parseStmt(stmt string) (string, bool) {
	if strings.HasSuffix(stmt, `\G`) {
		return stmt[:len(stmt)-2], true
	}
	return stmt, false
}
