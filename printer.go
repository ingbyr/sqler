package main

import (
	"database/sql"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
)

func NewPrinter() *Printer {
	outputFile, err := os.OpenFile("output.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	p := &Printer{
		outputFile: outputFile,
		jobs:       make(chan *Job, jobCacheSize),
	}
	go p.Run()
	return p
}

type Printer struct {
	outputFile *os.File
	jobs       chan *Job
}

func (p *Printer) WriteString(s string) (n int, err error) {
	fmt.Print(s)
	return p.outputFile.WriteString(s)
}

func (p *Printer) Write(b []byte) (n int, err error) {
	_, _ = os.Stdout.Write(b)
	return p.outputFile.Write(b)
}

func (p *Printer) Run() {
	for {
		select {
		case job := <-p.jobs:
			job.Executed.Wait()
			p.WriteString(fmt.Sprintf("%s %s\n", job.Prefix, job.Stmt))

			if job.Err != nil {
				p.WriteString(job.Err.Error())
				p.WriteString("\n\n")
				job.Printed.Done()
				continue
			}

			columns, _ := job.Result.Columns()
			table := tablewriter.NewWriter(p)
			lines := p.toStringSlice(job.Result)
			table.SetHeader(columns)
			for j := range lines {
				table.Append(lines[j])
			}
			table.Render()
			p.WriteString("\n")
			job.Printed.Done()
		}
	}
}

func (p *Printer) PrintJob(job *Job) {
	p.jobs <- job
}

func (p *Printer) CheckError(msg string, err error) {
	if err != nil {
		p.PrintError(msg, err)
		os.Exit(1)
	}
}

func (p *Printer) PrintError(msg string, err error) {
	p.WriteString("\n======= ERROR ========\n")
	p.WriteString(fmt.Sprintf("message: %s\n", msg))
	p.WriteString(fmt.Sprintf("error  : %v\n", err))
}

func (p *Printer) toStringSlice(rows *sql.Rows) [][]string {
	lines := make([][]string, 0)
	columns, err := rows.Columns()
	p.CheckError("Error getting columns from table", err)
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		p.CheckError("Error scanning rows from table", err)
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
	p.CheckError("Error scanning rows from table", rows.Err())
	return lines
}
