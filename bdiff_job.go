package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewBdiffJob(sqler *Sqler, schemas []string) Job {
	if len(schemas) == 0 {
		schemas = sqler.cfg.CommandsConfig.BdiffSchemas
	}
	return WrapJob(&BdiffJob{
		sqler:   sqler,
		schemas: schemas,
	})
}

type BdiffJob struct {
	sqler   *Sqler
	schemas []string
	*DefaultJob
}

func (job *BdiffJob) DoExec() error {
	baseDb := job.sqler.dbs[0]
	if err := os.Mkdir("bdiff", 0755); err != nil && !os.IsExist(err) {
		return err
	}
	// Compare
	for sid, schema := range job.schemas {
		// csv file
		csvFileName := fmt.Sprintf("bdiff/%s.csv", schema)
		file, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0665)
		if err != nil {
			panic(err)
		}
		csvFile := csv.NewWriter(file)
		// Columns and base row data
		query := "select * from " + schema
		rawBaseRows, err := baseDb.Query(query)
		if err != nil {
			return err
		}
		baseColumns, baseRows, err := convertSqlResults(rawBaseRows)
		if err != nil {
			return err
		}
		// Write csv columns header
		mustWriteToCsv(csvFile, baseColumns, "Table", "DataSource", "Type")
		for did, db := range job.sqler.dbs {
			if did == 0 {
				continue
			}
			// Compare data in another db
			dsKey := job.sqler.cfg.DataSources[did].DsKey()
			baseRowMap := rowResultToMap(baseRows)
			compare(csvFile, dsKey, schema, baseColumns, baseRowMap, db, query)
			csvFile.Flush()

			jobPrinter.PrintInfo(fmt.Sprintf("Compared %d/%d schema, %d/%d db", sid, len(schema), did, len(job.sqler.dbs)))
		}
		csvFile.Flush()
		if err := file.Close(); err != nil {
			return err
		}
		jobPrinter.PrintInfo("Csv file: " + csvFileName)
	}

	return nil
}

func (job *BdiffJob) SetWrapper(defaultJob *DefaultJob) {
	job.DefaultJob = defaultJob
}

func compare(csvFile *csv.Writer, dsKey string, schema string, baseColumns []string, baseRowMap map[string][]string, db *sql.DB, query string) {
	// Query target db row data
	rawRows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	columns, rows, err := convertSqlResults(rawRows)
	if err != nil {
		panic(err)
	}
	// Skip compare data step if has different columns
	if !sameSlices(baseColumns, columns) {
		mustWriteToCsv(csvFile, columns, schema, dsKey, "DIFF_TABLE")
		return
	}
	rowMap := rowResultToMap(rows)

	compareRows(csvFile, dsKey, schema, baseRowMap, rowMap)
}

func compareRows(csvFile *csv.Writer, dsKey string, schema string, baseRowMap map[string][]string, rowMap map[string][]string) {
	// Find extra rows or different rows
	for _, row := range rowMap {
		baseRow, ok := baseRowMap[row[0]]
		// Extra row
		if !ok {
			mustWriteToCsv(csvFile, row, schema, dsKey, "EXTRA")
			continue
		}
		// Different row
		if !sameSlices(baseRow, row) {
			mustWriteToCsv(csvFile, baseRow, schema, "base", "DIFF")
			mustWriteToCsv(csvFile, row, schema, dsKey, "DIFF")
			continue
		}
	}
	// Find missing rows
	for _, baseRow := range baseRowMap {
		if _, ok := rowMap[baseRow[0]]; !ok {
			mustWriteToCsv(csvFile, baseRow, schema, dsKey, "MISSING")
		}
	}
}

func sameSlices(slice1, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}
	return true
}

func mustWriteToCsv(csvFile *csv.Writer, data []string, extraHeaders ...string) {
	if len(extraHeaders) != 0 {
		data = append(extraHeaders, data...)
	}
	if err := csvFile.Write(data); err != nil {
		panic(err)
	}
}

func rowResultToMap(rows [][]string) map[string][]string {
	rowMap := make(map[string][]string, len(rows))
	for _, baseRow := range rows {
		id := baseRow[0]
		rowMap[id] = baseRow
	}
	return rowMap
}
