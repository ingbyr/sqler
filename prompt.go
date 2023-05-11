package main

import (
	"bufio"
	"github.com/c-bata/go-prompt"
	"os"
	"path/filepath"
	"sqler/pkg"
)

var (
	promptSuggest []prompt.Suggest
)

func completer(d prompt.Document) []prompt.Suggest {
	return prompt.FilterHasPrefix(promptSuggest, d.GetWordBeforeCursor(), true)
}

func initPromptSuggest(tms []*TableMeta, cms []*ColumnMeta) {
	commands := cliCommandSuggests()
	sqlKeywords := sqlKeyWords()
	customSuggests := loadCustomSuggests("prompt.txt")

	suggestSize := len(tms) + len(cms) + len(commands) + len(sqlKeywords) + len(customSuggests) + 8
	promptSuggest = make([]prompt.Suggest, 0, suggestSize)

	// Table meta
	for _, tm := range tms {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        tm.Name,
			Description: tm.Comment + "[table]",
		})
	}

	// Column meta
	for _, cm := range cms {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        cm.Name,
			Description: cm.Comment + "[" + cm.Type + "]",
		})
	}

	// App commands
	for _, cmd := range commands {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        cmd[0],
			Description: cmd[1],
		})
	}

	// Some sql keywords
	for _, kw := range sqlKeywords {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        kw,
			Description: "SQL key word",
		})
	}

	// Some customSuggests
	for _, custom := range customSuggests {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        custom,
			Description: "Custom",
		})
	}

	// Same dir sql files
	sqlFileNames := sqlFileNamesInCurrentDir()
	for _, sqlFileName := range sqlFileNames {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        sqlFileName,
			Description: "SQL file",
		})
	}
}

func cliCommandSuggests() [][]string {
	return pkg.CommandSuggests()
}

func sqlFileNamesInCurrentDir() []string {
	matches, err := filepath.Glob("*.sql")
	if err != nil {
		return nil
	}
	return matches
}

func sqlKeyWords() []string {
	return []string{
		"SELECT", "select", "UPDATE", "update", "INSERT INTO", "insert into", "WHERE", "where",
		"FROM", "from", "GROUP BY", "group by", "HAVING", "having", "LIMIT", "limit", "join", "JOIN",
		"left join", "LEFT JOIN", "right join", "RIGHT join"}
}

func loadCustomSuggests(promptFile string) []string {
	sqlFile, err := os.Open(promptFile)
	if err != nil {
		return nil
	}

	scanner := bufio.NewScanner(sqlFile)
	scanner.Split(bufio.ScanLines)
	suggests := make([]string, 0)
	for scanner.Scan() {
		suggests = append(suggests, scanner.Text())
	}
	return suggests
}
