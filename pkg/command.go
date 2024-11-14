package pkg

const (
	CmdSource     = "/source"
	CmdDatasource = "/datasource"
	CmdClear      = "/clear"
	CmdActive     = "/active"
	CmdCount      = "/count"
	CmdExportCsv  = "/export-csv"
	CmdLog        = "/log"
)

func CommandSuggests() [][]string {
	return [][]string{
		{CmdDatasource, "显示当前数据源"},
		{CmdSource, "执行SQL文件（foo.sql）"},
		{CmdClear, "清除当前输入的部分SQL"},
		{CmdActive, "激活其他配置文件（当前版本不可用）"},
		{CmdCount, "查询表中数据行数，不指定参数则从配置中读取（table_1 table_2 ... ）"},
		{CmdExportCsv, "导出SQL执行结果到CSV文件 (foo.csv \"select 1 from dual\" 或 foo.csv file.sql)"},
		{CmdLog, "显示当前日志路径"},
	}
}
