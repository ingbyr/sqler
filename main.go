package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/olekukonko/tablewriter"
	"os"
	"os/signal"
	"sqler/pkg"
	"strconv"
	"strings"
)

var (
	Version   string
	BuildTime string
	BuildBy   string
)

var (
	flagConfig      string
	flagSqlFile     string
	flagInteractive bool
	flagVersion     bool
	configFile      string
)

var (
	sqler        *Sqler
	jobPrinter   *JobPrinter
	sqlStmtCache *strings.Builder
)

func parseFlags() {
	flag.StringVar(&flagConfig, "c", "config.yml", "(config) 配置文件")
	flag.StringVar(&flagSqlFile, "f", "", "(file) sql文件路径")
	flag.BoolVar(&flagInteractive, "i", false, "(interactive) 交互模式")
	flag.BoolVar(&flagVersion, "v", false, "(version) 版本号")
	flag.Parse()
	configFile = flagConfig
}

func initQuitChan() chan os.Signal {
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, os.Kill)
	return quitChan
}

func initComponents() {
	initJobPrinter(false)
	initSqler(false)
}

func initJobPrinter(override bool) {
	if jobPrinter == nil || override {
		jobPrinter = NewJobPrinter()
	}
}

func initSqler(override bool) {
	if sqler == nil || override {
		cfg, err := pkg.LoadConfigFromFile(configFile)
		if err != nil {
			panic(err)
		}
		sqler = NewSqler(cfg, jobPrinter)
		if err := sqler.loadSchema(); err != nil {
			panic(err)
		}
		initPromptSuggest(sqler.tableMetas, sqler.columnMeats)
		sqlStmtCache = new(strings.Builder)
	}
}

func cli() {
	parseFlags()
	doActions := false

	if flagVersion {
		doActions = true
		fmt.Println("Version:", Version)
		fmt.Println("Build Time:", BuildTime)
		fmt.Println("Build By:", BuildBy)
		os.Exit(0)
	}

	if flagSqlFile != "" {
		doActions = true
		initComponents()
		jobPrinter.PrintInfo(fmt.Sprintf("Execute sql file: %s\n", flagSqlFile))
		execSql(true, LoadSqlFile(flagSqlFile)...)
	}

	if flagInteractive {
		doActions = true
		initComponents()
		p := prompt.New(
			executor,
			completer,
			prompt.OptionLivePrefix(func() (prefix string, useLivePrefix bool) {
				return currentPrefix(), true
			}),
			prompt.OptionTitle("sqler"),
			prompt.OptionBreakLineCallback(func(document *prompt.Document) {
				jobPrinter.LogInfo(currentPrefix() + document.Text)
			}),
		)
		p.Run()
	}

	if !doActions {
		flag.PrintDefaults()
	}
}

func currentPrefix() string {
	prefix := ""
	if sqlStmtCache.Len() > 0 {
		prefix = fmt.Sprintf("(%s) sql > ", configFile)
	} else {
		prefix = fmt.Sprintf("(%s) > ", configFile)
	}
	return prefix
}

func executor(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}

	if strings.HasPrefix(line, pkg.CmdSource) {
		files := strings.Split(line, " ")[1:]
		sourceSqlFiles(files)
		return
	}

	if strings.HasPrefix(line, pkg.CmdDatasource) {
		b := new(bytes.Buffer)
		table := tablewriter.NewWriter(b)
		table.SetHeader([]string{"ID", "URL", "Schema", "Enabled"})
		for i, ds := range sqler.cfg.DataSources {
			table.Append([]string{strconv.Itoa(i),
				ds.Url, ds.Schema, strconv.FormatBool(ds.Enabled)})
		}
		table.Render()
		jobPrinter.PrintInfo(b.String())
		return
	}

	if strings.HasPrefix(line, pkg.CmdClear) {
		sqlStmtCache = new(strings.Builder)
		return
	}

	if strings.HasPrefix(line, pkg.CmdActive) {
		configFiles := strings.Split(line, " ")[1:]
		if len(configFiles) != 1 {
			jobPrinter.PrintInfo("args 0 must be one string")
			return
		}
		configFile = configFiles[0]
		initSqler(true)
		return
	}

	if strings.HasPrefix(line, pkg.CmdCount) {
		schemas := strings.Split(line, " ")[1:]
		if len(schemas) == 0 {
			schemas = sqler.cfg.CommandsConfig.CountSchemas
		}
		countJob := NewCountJob(sqler, schemas)
		jobExecutor := NewJobExecutor(1, jobPrinter)
		jobExecutor.Start()
		jobExecutor.Submit(countJob, 0)
		jobExecutor.Shutdown(true)
		return
	}

	executable := strings.HasSuffix(line, ";")
	if executable {
		line = line[:len(line)-1]
	}
	sqlStmtCache.WriteString(line)
	if executable {
		execSql(false, sqlStmtCache.String())
		sqlStmtCache = new(strings.Builder)
	}
}

func sourceSqlFiles(files []string) {
	if len(files) == 0 {
		return
	}
	for _, file := range files {
		execSql(true, LoadSqlFile(file)...)
	}
}

func execSql(stopWhenError bool, sqlStmt ...string) {
	sqler.ExecPara(stopWhenError, sqlStmt...)
}

func main() {
	cli()
}
