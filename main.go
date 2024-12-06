package main

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sqler/pkg"
	"strconv"
	"strings"
	"sync"

	"github.com/elk-language/go-prompt"
	"github.com/olekukonko/tablewriter"
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
	flagOutputFile   string
	flagPara         bool
)

var (
	sqler        *Sqler
	printer      *CompositedPrinter
	sqlStmtCache *strings.Builder
)

func parseFlags() {
	flag.StringVar(&flagConfig, "c", "config.yml", "数据库配置文件")
	flag.StringVar(&flagSqlFile, "f", "", "待执行的SQL文件")
	flag.BoolVar(&flagInteractive, "i", false, "交互模式")
	flag.BoolVar(&flagVersion, "v", false, "打印版本号")
	flag.StringVar(&flagEnc, "enc", "", "aes加密")
	flag.StringVar(&flagDec, "dec", "", "aes解密")
	flag.BoolVar(&flagGenHexAesKey, "gen-key", false, "生成16进制的aes密钥")
	flag.StringVar(&flagHexAesKey, "key", "", "指定16进制格式的密钥")
	flag.BoolVar(&flagBdiff, "bdiff", false, "执行数据比对")
	flag.StringVar(&flagSchemas, "schemas", "", "数据比对的表 (table_a table_2 ...)")
	flag.IntVar(&flagMaxRowNumber, "max-row", 100000, "数据比对最大行数")
	flag.IntVar(&flagBatchRow, "batch-row", 0, "数据比对每批行数（默认0不限制）")
	flag.StringVar(&flagOutputFile, "o", "", "结果导出到文件")
	flag.BoolVar(&flagPara, "p", false, "并发执行模式")
	flag.Parse()
	configFile = flagConfig
}

func initComponents() {
	initJobPrinter(false)
	initSqler(false)
}

func initJobPrinter(override bool) {
	if printer == nil || override {
		printer = NewPrinter()
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
		sqler = NewSqler(cfg)
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
		return
	}

	if flagSqlFile != "" {
		doActions = true
		initComponents()
		printer.Info(fmt.Sprintf("Execute sql file: %s", flagSqlFile))
		if flagOutputFile == "" {
			stmts, err := LoadSqlFile(flagSqlFile)
			if err != nil {
				printer.Error("Failed to load sql file "+flagSqlFile, err)
				return
			}
			execSql(&JobCtx{StopWhenError: true}, stmts...)
		} else {
			if !strings.HasSuffix(flagOutputFile, ".csv") {
				printer.Info("Output file must be csv file")
				return
			}
			stmt, err := LoadOneSqlFile(flagSqlFile)
			if err != nil {
				printer.Error("Failed to load sql file "+flagSqlFile, err)
				return
			}
			csvFile, err := os.OpenFile(flagOutputFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
			if err != nil {
				panic(err)
			}
			defer csvFile.Close()
			jobCtx := &JobCtx{
				StopWhenError:      false,
				Serial:             !flagPara,
				ExportCsv:          true,
				CsvFileName:        csvFile.Name(),
				CsvFile:            csv.NewWriter(csvFile),
				CsvFileHeaderWrote: false,
				CsvFileLock:        &sync.Mutex{},
			}
			execSql(jobCtx, stmt)
		}
		return
	}

	if flagEnc != "" {
		key := loadAesKey()
		aes := pkg.NewAes(key, []byte(""))
		hex := aes.EncAsHex(flagEnc)
		fmt.Println(hex)
		return
	}

	if flagDec != "" {
		key := loadAesKey()
		aes := pkg.NewAes(key, []byte(""))
		data := aes.DecAsStr(flagDec)
		fmt.Println(data)
		return
	}

	if flagGenHexAesKey {
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			panic(err)
		}
		fmt.Println(hex.EncodeToString(bytes))
		return
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
		jobExecutor := NewJobExecutor(1)
		jobExecutor.Start()
		jobExecutor.Submit(bdiffJob, 0)
		jobExecutor.Shutdown(true)
		//printer.WaitForNoJob(true)
		return
	}

	if flagInteractive {
		doActions = true
		initComponents()
		p := prompt.New(
			executor,
			prompt.WithCompleter(completer),
			prompt.WithPrefixCallback(func() string {
				return currentPrefix()
			}),
			prompt.WithTitle("sqler"),
			prompt.WithBreakLineCallback(func(document *prompt.Document) {
				printer.Log(currentPrefix() + document.Text)
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
		printer.Info(b.String())
		return
	}

	if strings.HasPrefix(line, pkg.CmdClear) {
		sqlStmtCache = new(strings.Builder)
		return
	}

	if strings.HasPrefix(line, pkg.CmdActive) {
		configFiles := strings.Split(line, " ")[1:]
		if len(configFiles) != 1 {
			printer.Info("args 0 must be one string")
			return
		}
		configFile = configFiles[0]
		initSqler(true)
		return
	}

	if strings.HasPrefix(line, pkg.CmdCount) {
		parts := strings.Split(line, " ")[1:]
		if len(parts) < 1 {
			printer.Info("Please provide csv file name")
			return
		}
		csvFileName := parts[0]
		if !strings.HasSuffix(csvFileName, ".csv") {
			printer.Info("File name must end with .csv")
			return
		}
		schemes := parts[1:]
		if len(schemes) == 0 {
			parts = sqler.cfg.CommandsConfig.CountSchemas
		}
		countJob := NewCountJob(sqler, csvFileName, schemes)
		jobExecutor := NewJobExecutor(1)
		jobExecutor.Start()
		jobExecutor.Submit(countJob, 0)
		jobExecutor.Shutdown(true)
		return
	}

	if strings.HasPrefix(line, pkg.CmdExportCsv) {
		parts := splitBySpacesWithQuotes(line)
		if len(parts) != 3 {
			printer.Info("Invalid args")
			return
		}
		csvFileName := parts[1]
		if !strings.HasSuffix(csvFileName, ".csv") {
			printer.Info("File name must end with csv")
			return
		}
		csvFile, err := os.OpenFile(csvFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			printer.Error("Failed to create or open file "+csvFileName, err)
			return
		}
		defer csvFile.Close()
		var sqlStmt string
		sqlFileName := parts[2]
		if strings.HasSuffix(sqlFileName, ".sql") {
			sqlStmt, err = LoadOneSqlFile(sqlFileName)
			if err != nil {
				printer.Error("Failed to load sql "+sqlFileName, err)
				return
			}
		} else {
			sqlStmt, _ = strings.CutSuffix(sqlFileName, ";")
		}
		sqlJobCtx := &JobCtx{
			StopWhenError:      false,
			Serial:             false,
			ExportCsv:          true,
			CsvFileName:        csvFileName,
			CsvFile:            csv.NewWriter(csvFile),
			CsvFileHeaderWrote: false,
			CsvFileLock:        &sync.Mutex{},
		}
		execSql(sqlJobCtx, sqlStmt)
		return
	}

	if strings.HasPrefix(line, pkg.CmdLog) {
		printer.Info(printer.f.Name() + "\n")
		return
	}

	executable := strings.HasSuffix(line, ";")
	if executable {
		line = line[:len(line)-1]
	}
	sqlStmtCache.WriteString(line)
	if executable {
		execSql(
			&JobCtx{
				StopWhenError: false,
			},
			sqlStmtCache.String())
		sqlStmtCache = new(strings.Builder)
	}
}

func sourceSqlFiles(files []string) {
	if len(files) == 0 {
		return
	}
	for _, file := range files {
		stmts, err := LoadSqlFile(file)
		if err != nil {
			printer.Error("Failed to load sql file "+flagSqlFile, err)
			return
		}
		execSql(&JobCtx{StopWhenError: true}, stmts...)
	}
}

func execSql(jobCtx *JobCtx, sqlStmt ...string) {
	if jobCtx.Serial {
		sqler.ExecSerial(jobCtx, sqlStmt...)
	} else {
		sqler.ExecPara(jobCtx, sqlStmt...)
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

func handleExit() {
	rawModeOff := exec.Command("/bin/stty", "-raw", "echo")
	rawModeOff.Stdin = os.Stdin
	_ = rawModeOff.Run()
	rawModeOff.Wait()
}

func main() {
	defer handleExit()
	cli()
	if printer != nil {
		printer.Info("Execution log is in: " + printer.LogFilePath())
	}
}
