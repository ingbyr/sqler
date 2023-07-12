package main

import (
	"bufio"
	"os"
	"strings"
)

func LoadSqlFile(sqlFilePath string) []string {
	sqlFile, err := os.Open(sqlFilePath)
	if err != nil {
		jobPrinter.PrintError("Failed to open file "+sqlFilePath, err)
		return nil
	}
	jobPrinter.PrintInfo("Loading file " + sqlFilePath)
	stmts := LoadStmtsFromFile(sqlFile)
	jobPrinter.PrintInfo("Loaded file " + sqlFilePath)
	return stmts
}

func LoadStmtsFromFile(sqlFile *os.File) []string {
	scanner := bufio.NewScanner(sqlFile)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		//if i := bytes.IndexAny(data, ";\n"); i >= 0 {
		//	return i + 1, data[0:i], nil
		//}
		if i := strings.Index(string(data), ";\n"); i >= 0 {
			return i + 1, data[0:i], nil
		}
		if i := strings.Index(string(data), ";\r\n"); i >= 0 {
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
		if stmt != "" && !strings.HasPrefix(stmt, "#") {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
}
