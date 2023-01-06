package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"sync"
)

type Sqler struct {
	ctx context.Context
	cfg *Config
	dbs []*sql.DB
}

func NewSqler(cfg *Config) *Sqler {
	s := &Sqler{
		ctx: context.Background(),
		cfg: cfg,
	}
	for _, ds := range s.cfg.DataSources {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", ds.Username, ds.Password, ds.Url, ds.Schema, cfg.DataSourceArg)
		fmt.Printf("dsn: %s\n", dsn)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err)
		}
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
		fmt.Printf("[%d/%d] %s/%s\n", dbIdx+1, len(s.dbs), ds.Url, ds.Schema)
		for stmtIdx, stmt := range stmts {
			//fmt.Printf("  [%d/%d] exec> %s\n", stmtIdx+1, len(stmts), stmt)
			//rows, err := db.Query(stmt)
			//if err != nil {
			//	if stopWhenError {
			//		panic(err)
			//	} else {
			//		fmt.Printf("error: %v\n", err)
			//	}
			//	continue
			//}
			//for rows.Next() {
			//	columns, err := rows.Columns()
			//	if err != nil {
			//		panic(err)
			//	}
			//	fmt.Printf("  [%d/%d] result> %s\n", stmtIdx+1, len(stmts), strings.Join(columns, ", "))
			//}
			err := s.doExec(db, stmt, fmt.Sprintf("  [%d/%d]", stmtIdx+1, len(stmts)))
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
					err := s.doExec(db, stmt, prefix)
					if err != nil {
						if stopWhenError {
							quit <- os.Interrupt
							panic(err)
						} else {
							fmt.Printf("%s error", prefix)
						}
					}
				}
			}
			wg.Done()
		}(_db, _dbUri)
	}
	wg.Wait()
}

func (s *Sqler) doExec(db *sql.DB, stmt string, prefix string) error {
	fmt.Printf("%s exec> %s\n", prefix, stmt)
	rows, err := db.Query(stmt)
	if err != nil {
		return err
	}
	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s result> %s\n", prefix, strings.Join(columns, ", "))
	}
	return nil
}
