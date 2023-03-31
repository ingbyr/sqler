package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseJdbcUrl(t *testing.T) {
	a := assert.New(t)
	jdbcUrl := "jdbc:mysql://127.0.0.1:3306/sqler_demo?useUnicode=true&rewriteBatchedStatements=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai"
	url, schema := parseJdbcUrl(jdbcUrl)
	a.Equal(url, "127.0.0.1:3306")
	a.Equal(schema, "sqler_demo")
}
