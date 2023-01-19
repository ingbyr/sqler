package main

import (
	"database/sql"
	"sync"
)

type StmtJob struct {
	Stmt          string
	Prefix        string
	StopWhenError bool
	Db            *sql.DB
	Wg            *sync.WaitGroup
}
