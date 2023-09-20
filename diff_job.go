package main

import (
	"database/sql"
	"encoding/csv"
	"github.com/olekukonko/tablewriter"
	"os"
	"strings"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewDiffJob(sqler *Sqler, schema string, baseDBIdx int) Job {
	return WrapJob(&DiffJob{
		sqler:     sqler,
		schema:    schema,
		baseDBIdx: baseDBIdx,
	})
}

type DiffJob struct {
	sqler     *Sqler
	schema    string
	baseDBIdx int
	*DefaultJob
}

func (job *DiffJob) DoExec() error {
	baseDB := job.sqler.dbs[job.baseDBIdx]
	rows, err := baseDB.Query("select * from " + job.schema)
	if err != nil {
		panic(err)
	}
	columns, lines, err := convertSqlResults(rows)
	if err != nil {
		panic(err)
	}
	headers := make([]string, 0, len(columns)+1)
	headers = append(headers, "DataSource")
	for _, column := range columns {
		headers = append(headers, column)
	}
	// csv file
	file, err := os.OpenFile("diff-"+job.schema+".csv", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0665)
	if err != nil {
		panic(err)
	}
	csvFile := csv.NewWriter(file)
	if err := csvFile.Write(headers); err != nil {
		panic(err)
	}
	// table format
	table := tablewriter.NewWriter(job.output)
	table.SetHeader(headers)
	for i, db := range job.sqler.dbs {
		dsKey := job.sqler.cfg.DataSources[i].DsKey()
		job.compare(headers, lines, db, dsKey, csvFile, table)
	}
	csvFile.Flush()
	table.Render()
	return nil
}

func (job *DiffJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func (job *DiffJob) compare(headers []string, lines [][]string, db *sql.DB, dsKey string, csvWriter *csv.Writer, table *tablewriter.Table) {
	for _, line := range lines {
		id := line[0]
		rows, err := db.Query("select * from " + job.schema + " where id = '" + id + "'")
		if err != nil {
			panic(err)
		}
		_, targetLines := mustConvertSqlResults(rows)
		if len(targetLines) == 0 {
			// no data
			tipRow := job.generateTipRow(dsKey, len(headers), id, "[NO_DATA]")
			table.Append(tipRow)
			csvWriter.Write(tipRow)
		} else if len(targetLines) > 1 {
			// duplicated data
			tipRow := job.generateTipRow(dsKey, len(headers), id, "[DUPLICATED_DATA]")
			table.Append(tipRow)
			csvWriter.Write(tipRow)
		} else {
			targetLine := targetLines[0]
			if same, comparedLine := job.sameLine(dsKey, line, targetLine); !same {
				table.Append(comparedLine)
				csvWriter.Write(comparedLine)
			}
		}
	}
}

func (job *DiffJob) generateTipRow(dsKey string, n int, id string, tip string) []string {
	tipRow := make([]string, 0, n)
	tipRow = append(tipRow, dsKey)
	tipRow = append(tipRow, id)
	for i := 2; i < n; i++ {
		tipRow = append(tipRow, tip)
	}
	return tipRow
}

func (job *DiffJob) sameLine(dsKey string, originLine []string, targetLine []string) (bool, []string) {
	colSize := len(originLine)
	comparedLine := make([]string, 0, colSize+1)
	comparedLine = append(comparedLine, dsKey)
	same := true
	for i, originItem := range originLine {
		if originItem != targetLine[i] {
			same = false
		}
	}
	if !same {
		for i := 0; i < colSize; i++ {
			var b strings.Builder
			if i == 0 {
				b.WriteString(originLine[i])
			} else if targetLine[i] != originLine[i] {
				b.WriteString(targetLine[i])
				b.WriteString(" [")
				b.WriteString(originLine[i])
				b.WriteString("]")
			}
			comparedLine = append(comparedLine, b.String())
		}
	}
	return same, comparedLine
}
