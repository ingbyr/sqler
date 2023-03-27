package main

import (
	"github.com/c-bata/go-prompt"
)

var (
	promptSuggest []prompt.Suggest
)

func completer(d prompt.Document) []prompt.Suggest {
	return prompt.FilterHasPrefix(promptSuggest, d.GetWordBeforeCursor(), true)
}

func initPromptSuggest(tms []*TableMeta, cms []*ColumnMeta) {
	promptSuggest = make([]prompt.Suggest, 0, len(tms)+len(cms))

	for _, tm := range tms {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        tm.Name,
			Description: tm.Comment + "[table]",
		})
	}

	for _, cm := range cms {
		promptSuggest = append(promptSuggest, prompt.Suggest{
			Text:        cm.Name,
			Description: cm.Comment + "[" + cm.Type + "]",
		})
	}
}
