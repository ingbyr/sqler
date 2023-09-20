package main

import (
	"database/sql"
	"github.com/olekukonko/tablewriter"
	"sqler/pkg"
	"strings"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewDiffJob(sqler *Sqler, schema string) Job {
	return WrapJob(&DiffJob{
		sqler:  sqler,
		schema: schema,
	})
}

type DiffJob struct {
	sqler  *Sqler
	schema string
	*DefaultJob
}

func (job *DiffJob) DoExec() error {
	baseDB := job.sqler.dbs[0]
	rows, err := baseDB.Query("select * from " + job.schema)
	if err != nil {
		panic(err)
	}
	columns, lines, err := convertSqlResults(rows)
	if err != nil {
		panic(err)
	}
	for i, db := range job.sqler.dbs {
		dbCfg := job.sqler.cfg.DataSources[i]
		job.compare(columns, lines, db, dbCfg)
	}

	return nil
}

func (job *DiffJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

type DiffResult struct {
	ID    string
	Value string
}

func (job *DiffJob) compare(columns []string, lines [][]string, db *sql.DB, dsCfg *pkg.DataSourceConfig) {
	job.output.WriteString("Data Source: " + dsCfg.DsKey() + "\n")
	table := tablewriter.NewWriter(job.output)
	tableHeaders := make([]string, 0, len(columns))
	for _, column := range columns {
		tableHeaders = append(tableHeaders, column)
	}
	table.SetHeader(tableHeaders)

	for _, line := range lines {
		id := line[0]
		rows, err := db.Query("select * from " + job.schema + " where id = '" + id + "'")
		if err != nil {
			panic(err)
		}
		_, targetLines := mustConvertSqlResults(rows)

		if len(targetLines) == 0 {
			// no data
			tipRow := job.generateTipRow(len(tableHeaders), id, "[NO_DATA]")
			table.Append(tipRow)
		} else if len(targetLines) > 1 {
			// duplicated data
			tipRow := job.generateTipRow(len(tableHeaders), id, "[DUPLICATED_DATA]")
			table.Append(tipRow)
		} else {
			targetLine := targetLines[0]
			if same, comparedLine := job.sameLine(line, targetLine); !same {
				table.Append(comparedLine)
			}
		}
	}
	table.Render()
}

func (job *DiffJob) generateTipRow(n int, id string, tip string) []string {
	tipRow := make([]string, 0, n)
	tipRow = append(tipRow, id)
	for i := 1; i < n; i++ {
		tipRow = append(tipRow, tip)
	}
	return tipRow
}

func (job *DiffJob) sameLine(originLine []string, targetLine []string) (bool, []string) {
	colSize := len(originLine)
	comparedLine := make([]string, 0, colSize)
	same := true
	for i, originItem := range originLine {
		if originItem != targetLine[i] {
			same = false
		}
	}
	if !same {
		for i := 0; i < colSize; i++ {
			var b strings.Builder
			b.WriteString(targetLine[i])
			if targetLine[i] != originLine[i] {
				b.WriteString(" [")
				b.WriteString(originLine[i])
				b.WriteString("]")
			}
			comparedLine = append(comparedLine, b.String())
		}
	}
	return same, comparedLine
}
