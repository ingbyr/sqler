package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/olekukonko/tablewriter"
	"os"
	"sync"
)

var renderMu = &sync.Mutex{}

type Sqler struct {
	ctx     context.Context
	cfg     *Config
	dbs     []*sql.DB
	printer *Printer
}

func NewSqler(cfg *Config) *Sqler {
	s := &Sqler{
		ctx:     context.Background(),
		cfg:     cfg,
		dbs:     make([]*sql.DB, 0),
		printer: NewPrinter(),
	}
	for _, ds := range s.cfg.DataSources {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, cfg.DataSourceArg)
		s.printer.WriteString(fmt.Sprintf("dsn: %s\n", dsn))
		db, err := sql.Open("mysql", dsn)
		checkError("failed to connect to the db", err)
		if err = db.Ping(); err != nil {
			panic(err)
		}
		s.dbs = append(s.dbs, db)
	}
	return s
}

func (s *Sqler) Exec(stopWhenError bool, stmts ...string) {
	if stmts == nil || len(stmts) == 0 {
		return
	}
	for dbIdx, db := range s.dbs {
		ds := s.cfg.DataSources[dbIdx]
		s.printer.WriteString(fmt.Sprintf("[%d/%d] %s/%s\n", dbIdx+1, len(s.dbs), ds.Url, ds.Schema))
		for stmtIdx, stmt := range stmts {
			err := doExec(db, stmt, fmt.Sprintf("  [%d/%d]", stmtIdx+1, len(stmts)), s.printer)
			if err != nil {
				if stopWhenError {
					panic(err)
				} else {
					fmt.Printf("error: %v\n", err)
				}
			}
		}
	}
}

func (s *Sqler) ExecInParallel(stopWhenError bool, stmts ...string) {
	total := len(s.dbs) * len(stmts)
	idx := 0
	mu := &sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(s.dbs))
	for i, _db := range s.dbs {
		ds := s.cfg.DataSources[i]
		_dbUri := fmt.Sprintf("%s/%s", ds.Url, ds.Schema)
		go func(db *sql.DB, dbUri string) {
			for _, stmt := range stmts {
				select {
				case <-quit:
					return
				default:
					mu.Lock()
					idx++
					currId := idx
					mu.Unlock()
					prefix := fmt.Sprintf("[%d/%d](%s)", currId, total, dbUri)
					err := doExec(db, stmt, prefix, s.printer)
					if err != nil {
						if stopWhenError {
							quit <- os.Interrupt
							panic(err)
						} else {
							fmt.Printf("%s %v\n", prefix, err)
						}
					}
				}
			}
			wg.Done()
		}(_db, _dbUri)
	}
	wg.Wait()
}

func doExec(db *sql.DB, stmt string, prefix string, printer *Printer) error {
	rows, err := db.Query(stmt)
	if err != nil {
		return err
	}
	columns, _ := rows.Columns()
	table := tablewriter.NewWriter(printer)
	lines := toStringSlice(rows)
	table.SetHeader(columns)
	for i := range lines {
		table.Append(lines[i])
	}
	renderMu.Lock()
	printer.WriteString(fmt.Sprintf("\n%s exec> %s\n", prefix, stmt))
	table.Render()
	renderMu.Unlock()
	return nil
}

func toStringSlice(rows *sql.Rows) [][]string {
	// this is the slice that will be appended with rows from the table
	lines := make([][]string, 0)

	// Get column names
	columns, err := rows.Columns()
	checkError("Error getting columns from table", err)

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// now let's loop through the table lines and append them to the slice declared above
	for rows.Next() {
		// read the row on the table
		// each column value will be stored in the slice
		err = rows.Scan(scanArgs...)

		checkError("Error scanning rows from table", err)

		var value string
		var line []string

		for _, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			line = append(line, value)
		}

		lines = append(lines, line)
	}

	checkError("Error scanning rows from table", rows.Err())

	return lines
}
