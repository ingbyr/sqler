package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sqler/pkg"
	"strconv"
	"strings"
)

func NewBdiffJob(sqler *Sqler, schemas []string, maxRow int, batchRow int) Job {
	if len(schemas) == 0 {
		schemas = sqler.cfg.CommandsConfig.BdiffSchemas
	}
	skipColsMap := make(map[string]bool, len(sqler.cfg.CommandsConfig.BdiffSchemas))
	for _, skipCol := range sqler.cfg.CommandsConfig.BdiffSkipCols {
		skipColsMap[skipCol] = true
	}
	return &BdiffJob{
		sqler:       sqler,
		schemas:     schemas,
		maxRow:      maxRow,
		skipColsMap: skipColsMap,
		batchRow:    batchRow,
		BaseJob:     NewBaseJob(new(JobCtx)),
	}
}

type BdiffJob struct {
	sqler       *Sqler
	schemas     []string
	maxRow      int
	skipColsMap map[string]bool
	batchRow    int
	*BaseJob
}

type dataRow struct {
	cols     []string
	compared bool
}

func (job *BdiffJob) Exec() {
	if err := os.Mkdir("bdiff", 0755); err != nil && !os.IsExist(err) {
		if job.RecordError(err) {
			return
		}
	}
	baseDb := job.sqler.dbs[0]
	// Compare schemas
	for sid, schema := range job.schemas {
		// csv file
		csvFileName := fmt.Sprintf("bdiff/%s.csv", schema)
		file, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0665)
		if err != nil {
			panic(err)
		}
		csvFile := csv.NewWriter(file)

		printer.Info(fmt.Sprintf("[%s] Loading BASE data: %s", pkg.Now(), schema))
		// Skip if too many data
		_ = baseDb.Ping()
		rows, err := baseDb.Query(fmt.Sprintf("select count(*) from %s", schema))
		if job.RecordError(err) {
			return
		}
		_, result, err := convertSqlResults(rows)
		if job.RecordError(err) {
			return
		}
		rowNumber, err := strconv.Atoi(result[0][0])
		if job.RecordError(err) {
			return
		}
		if job.maxRow > 0 && job.maxRow < rowNumber {
			printer.Info(fmt.Sprintf("[%s] Skip comparsion because of too many data in %s (%d > %d)\n", pkg.Now(), schema, rowNumber, job.maxRow))
			continue
		}

		// Get base data
		query := "select * from " + schema
		rawBaseRows, err := baseDb.Query(query)
		if job.RecordError(err) {
			return
		}
		baseColumns, baseRows, err := convertSqlResults(rawBaseRows)
		if job.RecordError(err) {
			return
		}

		mustWriteToCsv(csvFile, baseColumns, "Table", "DataSource", "Type", "SQL")

		// Generate skipping cols index
		skipCol := make([]bool, len(baseColumns))
		for i, column := range baseColumns {
			if job.skipColsMap[column] {
				skipCol[i] = true
			}
		}
		// Base row map
		baseRowMap := rowResultToMap(baseRows)

		// Compare to other db
		for dbIdx, db := range job.sqler.dbs {
			if dbIdx == 0 {
				continue
			}
			printer.Info(fmt.Sprintf("[%s] Comparing table %s (%d/%d) at db %s (%d/%d) ... ", pkg.Now(),
				schema, sid+1, len(job.schemas), job.sqler.cfg.DataSources[dbIdx].DsKey(), dbIdx, len(job.sqler.dbs)-1))
			dsKey := job.sqler.cfg.DataSources[dbIdx].DsKey()
			// Compare
			compare(csvFile, dsKey, schema, baseColumns, baseRowMap, db, query, skipCol, job.batchRow)
			csvFile.Flush()
			printer.Info(fmt.Sprintf("[%s] Compared table %s (%d/%d) at db %s (%d/%d) ... ", pkg.Now(),
				schema, sid+1, len(job.schemas), job.sqler.cfg.DataSources[dbIdx].DsKey(), dbIdx, len(job.sqler.dbs)-1))
		}
		csvFile.Flush()
		if err := file.Close(); err != nil {
			job.RecordError(err)
			return
		}
		printer.Info(fmt.Sprintf("[%s] Saved to csv file: %s\n", pkg.Now(), csvFileName))
	}

	printer.Info(fmt.Sprintf("[%s] All bdiff jobs are jobWg", pkg.Now()))
}

func compare(csvFile *csv.Writer, dsKey string, schema string, baseColumns []string,
	baseRowMap map[string]*dataRow, db *sql.DB, query string, skipCol []bool, batchRow int) {
	offset := 0
	if batchRow == 0 {
		batchRow = math.MaxInt
	}
	for {
		limitQuery := fmt.Sprintf("%s limit %d offset %d", query, batchRow, offset)
		// Query target db row data
		_ = db.Ping()
		rawRows, err := db.Query(limitQuery)
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
		if len(rows) == 0 {
			break
		}
		rowMap := rowResultToMap(rows)
		compareRows(csvFile, dsKey, schema, baseColumns, baseRowMap, rowMap, skipCol)
		offset += batchRow
	}
	// Find missing rows
	for _, baseRow := range baseRowMap {
		if !baseRow.compared {
			insertSql := generateInsertSql(schema, baseColumns, baseRow.cols)
			mustWriteToCsv(csvFile, baseRow.cols, schema, dsKey, "MISSING", insertSql)
			// Reset row flags
			baseRow.compared = false
		}
	}
}

func compareRows(csvFile *csv.Writer, dsKey string, schema string, baseColumns []string,
	baseRowMap map[string]*dataRow, rowMap map[string]*dataRow, skipCol []bool) {

	// Find extra rows or different rows
	for _, row := range rowMap {
		baseRow, ok := baseRowMap[row.cols[0]]
		// Extra row
		if !ok {
			// Insert SQL
			insertSql := generateInsertSql(schema, baseColumns, row.cols)
			mustWriteToCsv(csvFile, row.cols, schema, dsKey, "EXTRA", insertSql)
			continue
		}
		// Different row
		if same, diff := sameRow(baseRow.cols, row.cols, skipCol); !same {
			mustWriteToCsv(csvFile, baseRow.cols, schema, "BASE", "DIFF", "")
			mustWriteToCsv(csvFile, diff, schema, dsKey, "DIFF", "")
		}
		baseRow.compared = true
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
	if !same {
		// Record id column
		diffRow[0] = row[0]
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

func rowResultToMap(rows [][]string) map[string]*dataRow {
	rowMap := make(map[string]*dataRow, len(rows))
	for _, baseRow := range rows {
		id := baseRow[0]
		rowMap[id] = &dataRow{
			cols:     baseRow,
			compared: false,
		}
	}
	return rowMap
}
