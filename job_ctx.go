package main

import (
	"context"
	"encoding/csv"
	"sync"
)

type JobCtx struct {
	ctx                context.Context
	Serial             bool
	StopWhenError      bool
	ExportCsv          bool
	CsvFileName        string
	CsvFile            *csv.Writer
	CsvFileHeaderWrote bool
	CsvFileLock        *sync.Mutex
}
