package main

import "database/sql"

func mustQueryAsString(db *sql.DB, query string, args ...any) ([]string, [][]string) {
	heads, results, err := queryAsString(db, query, args...)
	if err != nil {
		panic(err)
	}
	return heads, results
}

func queryAsString(db *sql.DB, query string, args ...any) ([]string, [][]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	return convertSqlResults(rows)
}

func convertSqlResults(rows *sql.Rows) ([]string, [][]string, error) {
	lines := make([][]string, 0)
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, nil, err
		}
		var value string
		var line []string
		for _, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			line = append(line, value)
		}
		lines = append(lines, line)
	}

	return columns, lines, nil
}
