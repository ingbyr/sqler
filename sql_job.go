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
	sqlColumns, sqlResultLines := job.convertSqlResults()
	if job.Err != nil {
		job.level = Error
		b.WriteString(job.Err.Error())
		b.WriteByte('\n')
		return b.Bytes()
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

func (job *SqlJob) convertSqlResults() ([]string, [][]string) {
	if job.Err != nil {
		return nil, nil
	}
	lines := make([][]string, 0)
	columns, err := job.SqlRows.Columns()
	if err != nil {
		job.Err = err
		return nil, nil
	}
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for job.SqlRows.Next() {
		if err = job.SqlRows.Scan(scanArgs...); err != nil {
			job.Err = err
			return nil, nil
		}
		var value string
		var line []string
		for _, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			line = append(line, value)
		}
		lines = append(lines, line)
	}

	if err = job.SqlRows.Err(); err != nil {
		job.Err = err
		return nil, nil
	}

	return columns, lines
}
