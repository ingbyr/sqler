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
	"sync"
)

var (
	Version   string
	BuildTime string
	BuildBy   string
)

var (
	flagSqlFile     string
	flagInteractive bool
	flagParallel    bool
	flagParallel0   bool
	flagVersion     bool
	quit            = initQuitChan()
)

var (
	initOnce = &sync.Once{}
	sqler    *Sqler
	printer  *Printer
)

func parseFlags() {
	flag.StringVar(&flagSqlFile, "f", "", "(file) sql文件路径")
	flag.BoolVar(&flagInteractive, "i", false, "(interactive) 交互模式")
	flag.BoolVar(&flagParallel, "p", false, "(parallel) 并行执行模式")
	flag.BoolVar(&flagParallel0, "p0", false, "(parallel0) 完全并行执行模式")
	flag.BoolVar(&flagVersion, "v", false, "(version) 版本号")
	flag.Parse()
}

func initQuitChan() chan os.Signal {
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, os.Kill)
	return quitChan
}

func initSqler() {
	initOnce.Do(func() {
		printer = NewPrinter()
		cfg, errYmL := pkg.LoadConfigFromFile("config.yml")
		if errYmL != nil {
			var errYaml error
			cfg, errYaml = pkg.LoadConfigFromFile("config.yaml")
			if errYaml != nil {
				panic(errYaml)
			}
		}
		sqler = NewSqler(cfg)
		if err := sqler.loadSchema(); err != nil {
			panic(err)
		}
		initPromptSuggest(sqler.tableMetas, sqler.columnMeats)
	})
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
		initSqler()
		printer.PrintInfo(fmt.Sprintf("Execute sql file: %s\n", flagSqlFile))
		execSql(true, LoadSqlFile(flagSqlFile)...)
	}

	if flagInteractive {
		doActions = true
		initSqler()
		p := prompt.New(
			executor,
			completer,
			prompt.OptionPrefix("> "),
			prompt.OptionTitle("sqler"),
		)
		p.Run()
	}

	if !doActions {
		flag.PrintDefaults()
	}
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
		printer.PrintInfo(b.String())
		return
	}

	execSql(false, line)
}

func sourceSqlFiles(files []string) {
	if len(files) == 0 {
		return
	}
	for _, file := range files {
		execSql(false, LoadSqlFile(file)...)
	}
}

func execSql(stopWhenError bool, sqlStmt ...string) {
	if flagParallel0 {
		sqler.ExecPara0(sqlStmt...)
	} else if flagParallel {
		sqler.ExecPara(stopWhenError, sqlStmt...)
	} else {
		sqler.ExecSync(stopWhenError, sqlStmt...)
	}
}

func main() {
	cli()
}
