package main

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/olekukonko/tablewriter"
	"os"
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
	flagConfig       string
	configFile       string
	flagSqlFile      string
	flagInteractive  bool
	flagVersion      bool
	flagEnc          string
	flagDec          string
	flagGenHexAesKey bool
	flagHexAesKey    string
	flagBdiff        bool
	flagSchemas      string
	flagMaxRowNumber int
	flagBatchRow     int
)

var (
	sqler        *Sqler
	comPrinter   *CompositedPrinter
	sqlStmtCache *strings.Builder
)

func parseFlags() {
	flag.StringVar(&flagConfig, "c", "config.yml", "(config) 配置文件")
	flag.StringVar(&flagSqlFile, "f", "", "(file) sql文件路径")
	flag.BoolVar(&flagInteractive, "i", false, "(interactive) 交互模式")
	flag.BoolVar(&flagVersion, "v", false, "(version) 版本号")
	flag.StringVar(&flagEnc, "enc", "", "(enc) aes加密")
	flag.StringVar(&flagDec, "dec", "", "(dec) aes解密")
	flag.BoolVar(&flagGenHexAesKey, "gen-key", false, "(generate key) 生成16进制的aes密钥")
	flag.StringVar(&flagHexAesKey, "key", "", "(key) hex aes key")
	flag.BoolVar(&flagBdiff, "bdiff", false, "better diff tool")
	flag.StringVar(&flagSchemas, "schemas", "", "schema1 schema2 ...")
	flag.IntVar(&flagMaxRowNumber, "max-row", 100000, "max row")
	flag.IntVar(&flagBatchRow, "batch-row", 0, "batch row")
	flag.Parse()
	configFile = flagConfig
}

func initComponents() {
	initJobPrinter(false)
	initSqler(false)
}

func initJobPrinter(override bool) {
	if comPrinter == nil || override {
		comPrinter = NewJobPrinter()
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
		key := loadAesKey()
		cfg, err := pkg.LoadConfigFromFile(configFile, pkg.NewAes(key, pkg.DefaultIV))
		if err != nil {
			panic(err)
		}
		sqler = NewSqler(cfg, comPrinter)
		if err := sqler.loadSchema(); err != nil {
			fmt.Println("Failed to load schema: " + err.Error())
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
		comPrinter.PrintInfo(fmt.Sprintf("Execute sql file: %s\n", flagSqlFile))
		execSql(&SqlJobCtx{StopWhenError: true}, LoadSqlFile(flagSqlFile)...)
	}

	if flagEnc != "" {
		key := loadAesKey()
		aes := pkg.NewAes(key, []byte(""))
		hex := aes.EncAsHex(flagEnc)
		fmt.Println(hex)
		os.Exit(0)
	}

	if flagDec != "" {
		key := loadAesKey()
		aes := pkg.NewAes(key, []byte(""))
		data := aes.DecAsStr(flagDec)
		fmt.Println(data)
		os.Exit(0)
	}

	if flagGenHexAesKey {
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			panic(err)
		}
		fmt.Println(hex.EncodeToString(bytes))
		os.Exit(0)
	}

	if flagBdiff {
		initComponents()
		var schemas []string
		if flagSchemas == "" {
			schemas = sqler.cfg.CommandsConfig.BdiffSchemas
		} else {
			schemas = strings.Split(flagSchemas, " ")
		}
		bdiffJob := NewBdiffJob(sqler, schemas, flagMaxRowNumber, flagBatchRow)
		jobExecutor := NewJobExecutor(1, comPrinter)
		jobExecutor.Start()
		jobExecutor.Submit(bdiffJob, 0)
		jobExecutor.Shutdown(true)
		//comPrinter.WaitForNoJob(true)
		return
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
				comPrinter.LogInfo(currentPrefix() + document.Text)
			}),
		)
		p.Run()
	}

	if !doActions {
		flag.PrintDefaults()
	}
}

func loadAesKey() []byte {
	hexAesKey := ""
	if flagHexAesKey != "" {
		hexAesKey = flagHexAesKey
	} else {
		hexAesKey = os.Getenv("SQLER_CFG_AES_KEY")
		if hexAesKey == "" {
			fmt.Println("Please set env variable 'SQLER_CFG_AES_KEY' or pass '-key HEX-AES-KEY'")
			os.Exit(1)
		}
	}
	aesKey, err := hex.DecodeString(hexAesKey)
	if err != nil {
		panic(err)
	}
	if len(aesKey) != 16 {
		panic("Not valid aes key: " + hexAesKey)
	}
	return aesKey
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
		comPrinter.PrintInfo(b.String())
		return
	}

	if strings.HasPrefix(line, pkg.CmdClear) {
		sqlStmtCache = new(strings.Builder)
		return
	}

	if strings.HasPrefix(line, pkg.CmdActive) {
		configFiles := strings.Split(line, " ")[1:]
		if len(configFiles) != 1 {
			comPrinter.PrintInfo("args 0 must be one string")
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
		jobExecutor := NewJobExecutor(1, comPrinter)
		jobExecutor.Start()
		jobExecutor.Submit(countJob, 0)
		jobExecutor.Shutdown(true)
		return
	}

	if strings.HasPrefix(line, pkg.CmdExportCsv) {
		parts := splitBySpacesWithQuotes(line)
		if len(parts) != 3 {
			comPrinter.PrintInfo("Invalid args")
			return
		}
		csvFileName := parts[1]
		if !strings.HasSuffix(csvFileName, ".csv") {
			comPrinter.PrintInfo("File name must end with csv")
			return
		}
		csvFile, err := os.OpenFile(csvFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		defer csvFile.Close()
		var sqlStmt string
		if strings.HasSuffix(parts[2], ".sql") {
			sqlFile, err := os.Open(parts[2])
			if err != nil {
				panic(err)
			}
			stmts := LoadStmtsFromFile(sqlFile)
			if len(stmts) != 1 {
				comPrinter.PrintInfo("Only support 1 sql in file")
				return
			}
			sqlStmt = stmts[0]
		} else {
			sqlStmt, _ = strings.CutSuffix(parts[2], ";")
		}
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
