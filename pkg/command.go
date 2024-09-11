package pkg

const (
	CmdSource     = "/source"
	CmdDatasource = "/datasource"
	CmdClear      = "/clear"
	CmdActive     = "/active"
	CmdCount      = "/count"
	CmdExportCsv  = "/export-csv"
)

func CommandSuggests() [][]string {
	return [][]string{
		{CmdDatasource, "Show current data sources"},
		{CmdSource, "Source sql files"},
		{CmdClear, "Clear sql"},
		{CmdActive, "Active config file"},
		{CmdCount, "Count data in schema"},
		{CmdExportCsv, "Export data to csv file (csv-file-name \"sql\")"},
	}
}
