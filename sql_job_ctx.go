package main

import "encoding/csv"

type SqlJobCtx struct {
	Serial             bool
	StopWhenError      bool
	ExportCsv          bool
	CsvFile            *csv.Writer
	CsvFileHeaderWrote bool
}
