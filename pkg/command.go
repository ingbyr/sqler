package pkg

const (
	CmdSource     = "/source"
	CmdDatasource = "/datasource"
	CmdClear      = "/clear"
	CmdActive     = "/active"
	CmdCount      = "/count"
	CmdDiff       = "/diff"
	CmdBdiff      = "/bdiff"
	CmdExportCsv  = "/export-csv"
)

func CommandSuggests() [][]string {
	return [][]string{
		{CmdDatasource, "Show current data sources"},
		{CmdSource, "Source sql files"},
		{CmdClear, "Clear sql"},
		{CmdActive, "Active config file"},
		{CmdCount, "Count data in schema"},
		{CmdDiff, "Show difference data line (schema [db idx])"},
		{CmdBdiff, "Show difference data line (schema1 schema2 ...)"},
		{CmdExportCsv, "Export data to csv file (csv-file-name \"sql\")"},
	}
}
