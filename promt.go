package main

import (
	"github.com/c-bata/go-prompt"
	"path/filepath"
)

var (
	promptSuggest []prompt.Suggest
)

func completer(d prompt.Document) []prompt.Suggest {
	return prompt.FilterHasPrefix(promptSuggest, d.GetWordBeforeCursor(), true)
}

func initPromptSuggest(tms []*TableMeta, cms []*ColumnMeta, commands [][]string, sqlKeywords []string) {
	promptSuggest = make([]prompt.Suggest, 0, len(tms)+len(cms)+len(commands)+len(sqlKeywords))

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

	// Same dir sql files
	sqlFileNames := sqlFileNamesInCurrentDir()
	for _, sqlFileName := range sqlFileNames {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        sqlFileName,
			Description: "SQL file",
		})
	}
}

func sqlFileNamesInCurrentDir() []string {
	matches, err := filepath.Glob("*.sql")
	if err != nil {
		return nil
	}
	return matches
}
