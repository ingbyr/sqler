package main

import (
	"context"
	"encoding/csv"
	"sync"
)

type SqlJobCtx struct {
	ctx                context.Context
	Printer            *CompositedPrinter
	Serial             bool
	StopWhenError      bool
	ExportCsv          bool
	CsvFileName        string
	CsvFile            *csv.Writer
	CsvFileHeaderWrote bool
	CsvFileLock        *sync.Mutex
}

func NewSqlJobCtx(printer *CompositedPrinter) *SqlJobCtx {
	return &SqlJobCtx{
		ctx:     context.Background(),
		Printer: printer,
	}
}
