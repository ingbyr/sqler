package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"sqler/pkg"
	"strconv"
	"strings"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewBdiffJob(sqler *Sqler, schemas []string, maxRow int) Job {
	if len(schemas) == 0 {
		schemas = sqler.cfg.CommandsConfig.BdiffSchemas
	}
	skipColsMap := make(map[string]bool, len(sqler.cfg.CommandsConfig.BdiffSchemas))
	for _, skipCol := range sqler.cfg.CommandsConfig.BdiffSkipCols {
		skipColsMap[skipCol] = true
	}
	return WrapJob(&BdiffJob{
		sqler:       sqler,
		schemas:     schemas,
		maxRow:      maxRow,
		skipColsMap: skipColsMap,
	})
}

type BdiffJob struct {
	sqler       *Sqler
	schemas     []string
	maxRow      int
	skipColsMap map[string]bool
	*DefaultJob
}

func (job *BdiffJob) DoExec() error {
	baseDb := job.sqler.dbs[0]
	if err := os.Mkdir("bdiff", 0755); err != nil && !os.IsExist(err) {
		return err
	}
	// Compare schemas
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
		if job.maxRow > 0 && job.maxRow < rowNumber {
			fmt.Printf("[%s] Skip comparsion because of too many data in %s (%d > %d)\n\n", pkg.Now(), schema, rowNumber, job.maxRow)
			continue
		}
		// Get base data
		query := "select * from " + schema
		rawBaseRows, err := baseDb.Query(query)
		if err != nil {
			return err
		}
		baseColumns, baseRows, err := convertSqlResults(rawBaseRows)
		if err != nil {
			return err
		}
		mustWriteToCsv(csvFile, baseColumns, "Table", "DataSource", "Type", "SQL")

		// Generate skipping cols index
		skipCol := make([]bool, len(baseColumns))
		for i, column := range baseColumns {
			if job.skipColsMap[column] {
				skipCol[i] = true
			}
		}

		// Compare to other db
		for dbIdx, db := range job.sqler.dbs {
			if dbIdx == 0 {
				continue
			}
			fmt.Printf("[%s] Comparing table %s (%d/%d) at db %s (%d/%d) ... ", pkg.Now(),
				schema, sid+1, len(job.schemas), job.sqler.cfg.DataSources[dbIdx].DsKey(), dbIdx, len(job.sqler.dbs)-1)
			dsKey := job.sqler.cfg.DataSources[dbIdx].DsKey()
			baseRowMap := rowResultToMap(baseRows)

			compare(csvFile, dsKey, schema, baseColumns, baseRowMap, db, query, skipCol)
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

func compare(csvFile *csv.Writer, dsKey string, schema string, baseColumns []string,
	baseRowMap map[string][]string, db *sql.DB, query string, skipCol []bool) {

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
	if !sameCols(baseColumns, columns) {
		mustWriteToCsv(csvFile, columns, schema, dsKey, "DIFF_TABLE", "")
		return
	}
	rowMap := rowResultToMap(rows)

	compareRows(csvFile, dsKey, schema, baseColumns, baseRowMap, rowMap, skipCol)
}

func compareRows(csvFile *csv.Writer, dsKey string, schema string, baseColumns []string,
	baseRowMap map[string][]string, rowMap map[string][]string, skipCol []bool) {

	// Find extra rows or different rows
	for _, row := range rowMap {
		baseRow, ok := baseRowMap[row[0]]
		// Extra row
		if !ok {
			// Insert SQL
			insertSql := generateInsertSql(schema, baseColumns, row)
			mustWriteToCsv(csvFile, row, schema, dsKey, "EXTRA", insertSql)
			continue
		}
		// Different row
		if same, diff := sameRow(baseRow, row, skipCol); !same {
			mustWriteToCsv(csvFile, baseRow, schema, "BASE", "DIFF", "")
			mustWriteToCsv(csvFile, diff, schema, dsKey, "DIFF", "")
			continue
		}
	}
	// Find missing rows
	for _, baseRow := range baseRowMap {
		if _, ok := rowMap[baseRow[0]]; !ok {
			insertSql := generateInsertSql(schema, baseColumns, baseRow)
			mustWriteToCsv(csvFile, baseRow, schema, dsKey, "MISSING", insertSql)
		}
	}
}

func generateInsertSql(schema string, columns []string, row []string) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(schema)
	sb.WriteString("(")
	sb.WriteString(strings.Join(columns, ","))
	sb.WriteString(") VALUES (")
	for i, col := range row {
		if col == "NULL" {
			sb.WriteString("null")
		} else {
			sb.WriteString("'" + col + "'")
		}
		if i != len(row)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func sameCols(baseCols, cols []string) bool {
	if len(baseCols) != len(cols) {
		return false
	}
	for i, col := range cols {
		if col != baseCols[i] {
			return false
		}
	}
	return true
}

func sameRow(baseRow, row []string, skipCol []bool) (bool, []string) {
	diffRow := make([]string, len(baseRow))
	same := true
	if len(baseRow) != len(row) {
		same = false
		diffRow = row
		return same, diffRow
	}
	for i := range baseRow {
		if baseRow[i] != row[i] && !skipCol[i] {
			same = false
			diffRow[i] = row[i]
		} else {
			diffRow[i] = "/"
		}
	}
	return same, diffRow
}

func mustWriteToCsv(csvFile *csv.Writer, data []string, schema, dsKey, diffType, sql string) {
	writeToCsv(csvFile, data, schema, dsKey, diffType, sql)
}

func writeToCsv(csvFile *csv.Writer, data []string, extraHeaders ...string) {
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
