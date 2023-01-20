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
	printer *Printer
	dbSize  int
	dbs     []*sql.DB
	sjs     []chan *StmtJob
}

func NewSqler(cfg *Config) *Sqler {
	s := &Sqler{
		ctx:     context.Background(),
		cfg:     cfg,
		printer: NewPrinter(),
		dbSize:  len(cfg.DataSources),
		dbs:     make([]*sql.DB, len(cfg.DataSources)),
		sjs:     make([]chan *StmtJob, len(cfg.DataSources)),
	}
	// Init db and stmt job chan
	for i, ds := range s.cfg.DataSources {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, cfg.DataSourceArg)
		s.printer.WriteString(fmt.Sprintf("dsn: %s\n", dsn))
		db, err := sql.Open("mysql", dsn)
		checkError("failed to connect to the db", err)
		if err = db.Ping(); err != nil {
			panic(err)
		}
		s.dbs[i] = db
		s.sjs[i] = make(chan *StmtJob)
	}
	// Listen stmt job chan
	for _, sc := range s.sjs {
		go func(stmtChan chan *StmtJob) {
			for {
				select {
				case <-quit:
					return
				case sj := <-stmtChan:
					err := doExec(sj.Db, sj.Stmt, sj.Prefix, s.printer)
					if err != nil {
						if sj.StopWhenError {
							quit <- os.Interrupt
						}
					}
					sj.Wg.Done()
				}
			}
		}(sc)
	}
	return s
}

// ExecSync executes sql in turn (each sql and database)
func (s *Sqler) ExecSync(stopWhenError bool, stmts ...string) {
	if stmts == nil || len(stmts) == 0 {
		return
	}
	for dbIdx, db := range s.dbs {
		ds := s.cfg.DataSources[dbIdx]
		for stmtIdx, stmt := range stmts {
			sj := &StmtJob{
				Stmt: stmt,
				Prefix: fmt.Sprintf("[%d/%d %d/%d] (%s/%s)",
					dbIdx+1, s.dbSize, stmtIdx+1, len(stmts), ds.Url, ds.Schema),
				StopWhenError: stopWhenError,
				Db:            db,
				Wg:            &sync.WaitGroup{},
			}
			sj.Wg.Add(1)
			s.sjs[dbIdx] <- sj
			sj.Wg.Wait()
		}
	}
}

// ExecPara executes sql in parallel (each database)
func (s *Sqler) ExecPara(stopWhenError bool, stmts ...string) {
	for stmtIdx, stmt := range stmts {
		wg := &sync.WaitGroup{}
		wg.Add(s.dbSize)
		for dbIdx, db := range s.dbs {
			ds := s.cfg.DataSources[dbIdx]
			sj := &StmtJob{
				Stmt: stmt,
				Prefix: fmt.Sprintf("[%d/%d %d/%d] (%s/%s)",
					dbIdx+1, s.dbSize, stmtIdx+1, len(stmts), ds.Url, ds.Schema),
				StopWhenError: stopWhenError,
				Db:            db,
				Wg:            wg,
			}
			s.sjs[dbIdx] <- sj
		}
		wg.Wait()
	}
}

func (s *Sqler) ExecPara0(stopWhenError bool, stmts ...string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(stmts) * s.dbSize)
	for stmtIdx, stmt := range stmts {
		for dbIdx, db := range s.dbs {
			ds := s.cfg.DataSources[dbIdx]
			sj := &StmtJob{
				Stmt: stmt,
				Prefix: fmt.Sprintf("[%d/%d %d/%d] (%s/%s)",
					dbIdx+1, s.dbSize, stmtIdx+1, len(stmts), ds.Url, ds.Schema),
				StopWhenError: stopWhenError,
				Db:            db,
				Wg:            wg,
			}
			s.sjs[dbIdx] <- sj
		}
	}
	wg.Wait()
}

func doExec(db *sql.DB, stmt string, prefix string, printer *Printer) error {
	rows, err := db.Query(stmt)
	if err != nil {
		renderMu.Lock()
		printer.WriteString(fmt.Sprintf("\n%s exec> %s\n", prefix, stmt))
		printer.WriteString(err.Error())
		printer.WriteString("\n")
		renderMu.Unlock()
		return err
	}
	columns, _ := rows.Columns()
	table := tablewriter.NewWriter(printer)
	lines := toStringSlice(rows)
	table.SetHeader(columns)
	for j := range lines {
		table.Append(lines[j])
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
