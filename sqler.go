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

func (s *Sqler) Exec(stopWhenError bool, stmts ...string) {
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

func (s *Sqler) ExecInParallel(stopWhenError bool, stmts ...string) {
	total := len(s.dbs) * len(stmts)
	cntOk := 0
	cntFailed := 0
	cntMu := &sync.Mutex{}
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
					prefix := fmt.Sprintf("(%s)", dbUri)
					err := doExec(db, stmt, prefix, s.printer)
					cntMu.Lock()
					if err != nil {
						cntFailed++
						if stopWhenError {
							quit <- os.Interrupt
							os.Exit(1)
						}
					} else {
						cntOk++
					}
					cntMu.Unlock()
				}
			}
			wg.Done()
		}(_db, _dbUri)
	}
	wg.Wait()
	fmt.Printf("\n[Total %d of %d statements have been executed. Total %d has been failed]\n",
		cntOk, total, cntFailed)
}

func (s *Sqler) ExecInParallel0(stopWhenError bool, stmts ...string) {
	//total := len(stmts)
	//cntOk := 0
	//cntFailed := 0
	//cntMu := &sync.Mutex{}
	//wg := sync.WaitGroup{}
	//wg.Add(len(s.dbs))
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
