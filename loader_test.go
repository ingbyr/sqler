package main

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLoadSqlFile(t *testing.T) {
	as := assert.New(t)
	sqlFile, err := os.Open("tmp.sql")
	as.NoError(err)
	stmts, _ := LoadStmtsFromFile(sqlFile)
	as.Equal(5, len(stmts))
}
