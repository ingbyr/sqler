package pkg

const (
	CmdSource     = "/source"
	CmdDatasource = "/datasource"
	CmdClear      = "/clear"
	CmdActive     = "/active"
	CmdCount      = "/count"
	CmdDiff       = "/diff"
)

func CommandSuggests() [][]string {
	return [][]string{
		{CmdDatasource, "Show current data sources"},
		{CmdSource, "Source sql files"},
		{CmdClear, "Clear sql"},
		{CmdActive, "Active config file"},
		{CmdCount, "Count data in schema"},
		{CmdDiff, "Show difference data line (schema [db idx])"},
	}
}
