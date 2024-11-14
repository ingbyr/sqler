package main

import "encoding/csv"

type SqlJobCtx struct {
	Serial             bool
	StopWhenError      bool
	ExportCsv          bool
	CsvFileName        string
	CsvFile            *csv.Writer
	CsvFileHeaderWrote bool
}
