package main

import (
	"database/sql"
	"sync"
)

type Job struct {
	Stmt     string
	Db       *sql.DB
	Prefix   string
	Executed *sync.WaitGroup
	Printed  *sync.WaitGroup
	Result   *sql.Rows
	Err      error
}
