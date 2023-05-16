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
	flagParallel    bool
	flagParallel0   bool
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
	flag.BoolVar(&flagParallel, "p", true, "(parallel) 并行执行模式")
	flag.BoolVar(&flagParallel0, "p0", false, "(parallel0) 完全并行执行模式")
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
		sqler = NewSqler(cfg)
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
				jobPrinter.LogInfo(fmt.Sprintf("%s %s", currentPrefix(), document.Text))
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

	// Source sql files
	if strings.HasPrefix(line, pkg.CmdSource) {
		files := strings.Split(line, " ")[1:]
		sourceSqlFiles(files)
		return
	}

	// Show current data source
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

	// Clear sql cache
	if pkg.CmdClear == line {
		sqlStmtCache = new(strings.Builder)
		return
	}

	// Active another config
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
	if flagParallel0 {
		sqler.ExecPara0(sqlStmt...)
	} else if flagParallel {
		sqler.ExecPara(stopWhenError, sqlStmt...)
	}
}

func main() {
	cli()
}
