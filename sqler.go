package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
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
			fmt.Printf("  [%d/%d] exec> %s\n", stmtIdx+1, len(stmts), stmt)
			rows, err := db.Query(stmt)
			if err != nil {
				if stopWhenError {
					panic(err)
				} else {
					fmt.Printf("error: %v\n", err)
				}
				continue
			}
			for rows.Next() {
				columns, err := rows.Columns()
				if err != nil {
					panic(err)
				}
				fmt.Printf("  [%d/%d] result> %s\n", stmtIdx+1, len(stmts), strings.Join(columns, ", "))
			}
		}
	}
}
