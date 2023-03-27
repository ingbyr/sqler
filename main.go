package main

import (
	"flag"
	"fmt"
	"github.com/c-bata/go-prompt"
	"os"
	"os/signal"
	"strings"
	"sync"
)

var (
	Version   string
	BuildTime string
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
		cfg := LoadConfig("jdbc.properties")
		sqler = NewSqler(cfg)
		if err := sqler.loadSchema(); err != nil {
			panic(err)
		}
		initPromptSuggest(
			sqler.tableMetas,
			sqler.columnMeats,
			[][]string{{"/q", "Quit"}, {"/source", "Source sql files"}},
			[]string{"SELECT", "select", "UPDATE", "update", "INSERT INTO", "insert into", "WHERE", "where",
				"FROM", "from", "GROUP BY", "group by", "HAVING", "having", "LIMIT", "limit"},
		)
	})
}

func cli() {
	parseFlags()
	doActions := false

	if flagVersion {
		doActions = true
		fmt.Println("Version:", Version)
		fmt.Println("Build Time:", BuildTime)
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
			prompt.OptionTitle("sqler"))
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

	if line == "/q" {
		os.Exit(0)
	}

	if strings.HasPrefix(line, "/source") {
		files := strings.Split(line, " ")[1:]
		for _, file := range files {
			execSql(false, LoadSqlFile(file)...)
		}
		return
	}

	execSql(false, line)
}

func execSql(stopWhenError bool, sqlStmt ...string) {
	if flagParallel0 {
		sqler.ExecPara0(sqlStmt...)
	} else if flagParallel {
		sqler.ExecPara(false, sqlStmt...)
	} else {
		sqler.ExecSync(false, sqlStmt...)
	}
}

func main() {
	cli()
}
