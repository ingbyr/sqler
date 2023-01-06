package main

import (
	"bufio"
	"bytes"
	"os"
	"strings"
)

func LoadSqlFile(sqlFilePath string) []string {
	sqlFile, err := os.Open(sqlFilePath)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(sqlFile)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexAny(data, ";"); i >= 0 {
			return i + 1, data[0:i], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})
	stmts := make([]string, 0)
	for scanner.Scan() {
		stmt := strings.Replace(scanner.Text(), "\r\n", " ", -1)
		stmt = strings.Replace(stmt, "\n", " ", -1)
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
}
