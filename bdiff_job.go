package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"sqler/pkg"
	"strconv"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewBdiffJob(sqler *Sqler, schemas []string, maxRow int) Job {
	if len(schemas) == 0 {
		schemas = sqler.cfg.CommandsConfig.BdiffSchemas
	}
	return WrapJob(&BdiffJob{
		sqler:   sqler,
		schemas: schemas,
		maxRow:  maxRow,
	})
}

type BdiffJob struct {
	sqler   *Sqler
	schemas []string
	maxRow  int
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

		fmt.Printf("[%s] Loading BASE data: %s\n", pkg.Now(), schema)
		// Skip if too many data
		rows, err := baseDb.Query(fmt.Sprintf("select count(*) from %s", schema))
		if err != nil {
			return err
		}
		_, result, err := convertSqlResults(rows)
		if err != nil {
			return err
		}
		rowNumber, err := strconv.Atoi(result[0][0])
		if err != nil {
			return err
		}
		if job.maxRow < rowNumber {
			fmt.Printf("[%s] Skip comparsion because of too many data in %s (%d > %d)\n\n", pkg.Now(), schema, rowNumber, job.maxRow)
			continue
		}
		// Start compare data
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
		for dbIdx, db := range job.sqler.dbs {
			if dbIdx == 0 {
				continue
			}
			fmt.Printf("[%s] Comparing table %s (%d/%d) at db %s (%d/%d) ... ", pkg.Now(),
				schema, sid+1, len(job.schemas), job.sqler.cfg.DataSources[dbIdx].DsKey(), dbIdx, len(job.sqler.dbs)-1)
			// Compare data in another db
			dsKey := job.sqler.cfg.DataSources[dbIdx].DsKey()
			baseRowMap := rowResultToMap(baseRows)
			compare(csvFile, dsKey, schema, baseColumns, baseRowMap, db, query)
			csvFile.Flush()
			fmt.Printf("Done\n")
		}
		csvFile.Flush()
		if err := file.Close(); err != nil {
			return err
		}
		fmt.Printf("[%s] Saved to csv file: %s\n\n", pkg.Now(), csvFileName)
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
	if same, _ := sameRow(baseColumns, columns); !same {
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
		if same, diff := sameRow(baseRow, row); !same {
			mustWriteToCsv(csvFile, baseRow, schema, "BASE", "DIFF")
			mustWriteToCsv(csvFile, diff, schema, dsKey, "DIFF")
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

func sameRow(baseRow, row []string) (bool, []string) {
	var diffRow []string
	if len(baseRow) != len(row) {
		diffRow = row
		return false, diffRow
	}
	for i := range baseRow {
		if baseRow[i] != row[i] {
			if len(diffRow) == 0 {
				diffRow = make([]string, len(baseRow))
			}
			diffRow[i] = row[i]
		}
	}
	return len(diffRow) == 0, diffRow
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
