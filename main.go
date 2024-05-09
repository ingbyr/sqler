package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sqler/pkg"
	"strconv"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/olekukonko/tablewriter"
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
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			os.Exit(1)
		}
	}()
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
		execSql(&SqlJobCtx{StopWhenError: true}, LoadSqlFile(flagSqlFile)...)
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

	if strings.HasPrefix(line, pkg.CmdDiff) {
		cmdParts := strings.Split(line, " ")
		schema := cmdParts[1]
		baseDbIdx := 0
		var err error
		if len(cmdParts) == 3 {
			baseDbIdx, err = strconv.Atoi(cmdParts[2])
			if err != nil {
				panic(err)
			}
		}
		diffJob := NewDiffJob(sqler, schema, baseDbIdx)
		jobExecutor := NewJobExecutor(1, jobPrinter)
		jobExecutor.Start()
		jobExecutor.Submit(diffJob, 0)
		jobExecutor.Shutdown(true)
		return
	}

	if strings.HasPrefix(line, pkg.CmdExportCsv) {
		parts := splitBySpacesWithQuotes(line)
		if len(parts) != 3 {
			jobPrinter.PrintInfo("Invalid args")
			return
		}
		csvFileName := parts[1]
		if !strings.HasSuffix(csvFileName, ".csv") {
			jobPrinter.PrintInfo("File name must end with csv")
			return
		}
		csvFile, err := os.OpenFile(csvFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		defer csvFile.Close()
		sqlStmt, _ := strings.CutSuffix(parts[2], ";")
		sqlJobCtx := &SqlJobCtx{
			StopWhenError:      false,
			Serial:             true,
			ExportCsv:          true,
			CsvFile:            csv.NewWriter(csvFile),
			CsvFileHeaderWrote: false,
		}
		execSql(sqlJobCtx, sqlStmt)
		return
	}

	executable := strings.HasSuffix(line, ";")
	if executable {
		line = line[:len(line)-1]
	}
	sqlStmtCache.WriteString(line)
	if executable {
		execSql(&SqlJobCtx{StopWhenError: false}, sqlStmtCache.String())
		sqlStmtCache = new(strings.Builder)
	}
}

func sourceSqlFiles(files []string) {
	if len(files) == 0 {
		return
	}
	for _, file := range files {
		execSql(&SqlJobCtx{StopWhenError: true}, LoadSqlFile(file)...)
	}
}

func execSql(opts *SqlJobCtx, sqlStmt ...string) {
	if opts.Serial {
		sqler.ExecSerial(opts, sqlStmt...)
	} else {
		sqler.ExecPara(opts, sqlStmt...)
	}
}

func splitBySpacesWithQuotes(input string) []string {
	var parts []string
	inQuotes := false
	currentPart := ""

	for _, char := range input {
		if char == '"' {
			inQuotes = !inQuotes
		} else if char == ' ' && !inQuotes {
			if currentPart != "" {
				parts = append(parts, currentPart)
				currentPart = ""
			}
		} else {
			currentPart += string(char)
		}
	}

	if currentPart != "" {
		parts = append(parts, currentPart)
	}

	return parts
}

func main() {
	cli()
}
