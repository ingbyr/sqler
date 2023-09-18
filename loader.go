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
	stmts := make([]string, 0)
	var builder strings.Builder
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "--") {
			continue
		}
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
