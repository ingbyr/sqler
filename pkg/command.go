package pkg

const (
	CmdSource     = "/source"
	CmdDatasource = "/datasource"
	CmdClear      = "/clear"
	CmdActive     = "/active"
)

func CommandSuggests() [][]string {
	return [][]string{
		{CmdDatasource, "Show current data sources"},
		{CmdSource, "Source sql files"},
		{CmdClear, "Clear sql"},
		{CmdActive, "Active config file"},
	}
}
