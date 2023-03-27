package main

import _ "embed"

var (
	//go:embed sql/query_table_metas.sql
	stmtQueryTableMetas string

	//go:embed sql/query_column_metas.sql
	stmtQueryColumnMetas string
)
