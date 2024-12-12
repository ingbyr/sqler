package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

func LoadOneSqlFile(sqlFilePath string) (string, error) {
	stmts, err := LoadSqlFile(sqlFilePath)
	if err != nil {
		return "", err
	}
	if len(stmts) != 1 {
		err := errors.New("only support 1 sql in file")
		return "", err
	}
	return stmts[0], nil
}

func LoadSqlFile(sqlFilePath string) ([]string, error) {
	sqlFile, err := os.Open(sqlFilePath)
	if err != nil {
		return nil, err
	}
	printer.Info("Loading file " + sqlFilePath)
	stmts := LoadStmtsFromFile(sqlFile)
	printer.Info("Loaded file " + sqlFilePath)
	return stmts, nil
}

func LoadStmtsFromFile(sqlFile *os.File) []string {
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
	return stmts
}
