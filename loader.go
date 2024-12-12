package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

func LoadSqlFile(sqlFilePath string) ([]string, error) {
	sqlFile, err := os.Open(sqlFilePath)
	if err != nil {
		return nil, err
	}
	printer.Info("Loading file " + sqlFilePath)
	stmts, e := LoadStmtsFromFile(sqlFile)
	printer.Info("Loaded file " + sqlFilePath)
	return stmts, e
}

func LoadStmtsFromFile(sqlFile *os.File) ([]string, error) {
	scanner := bufio.NewScanner(sqlFile)
	stmts := make([]string, 0)
	var builder strings.Builder
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Comment line
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "--") {
			continue
		}
		// Sql end line;
		if strings.HasSuffix(line, ";") {
			builder.WriteString(line[:len(line)-1])
			stmts = append(stmts, builder.String())
			builder.Reset()
			continue
		}
		if line != "" {
			builder.WriteString(line)
			builder.Write([]byte(" "))
		}
	}
	if builder.Len() > 0 {
		return stmts, errors.New("Sql must end with ';'")
	}
	return stmts, nil
}
