package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
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
	jobCacheSize    = 32
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

func main() {
	parseFlags()

	if flagVersion {
		fmt.Println("version:", Version)
		fmt.Println("build:", BuildTime)
		os.Exit(0)
	}

	cfg := LoadConfig("jdbc.properties")
	sqler := NewSqler(cfg)

	if flagSqlFile != "" {
		fmt.Printf("execute sql from file: %s\n", flagSqlFile)
		if flagParallel0 {
			sqler.ExecPara0(LoadSqlFile(flagSqlFile)...)
		} else if flagParallel {
			sqler.ExecPara(true, LoadSqlFile(flagSqlFile)...)
		} else {
			sqler.ExecSync(true, LoadSqlFile(flagSqlFile)...)
		}
	}

	if flagInteractive {
		fmt.Println("start mysql shell ...")
		scanner := bufio.NewReader(os.Stdin)
		for {
			select {
			case <-quit:
				os.Exit(0)
			default:
				fmt.Printf("> ")
				line, err := scanner.ReadString('\n')
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					} else {
						panic(err)
					}
				}
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if flagParallel0 {
					sqler.ExecPara0(line)
				} else if flagParallel {
					sqler.ExecPara(false, line)
				} else {
					sqler.ExecSync(false, line)
				}
			}
		}
	}

}
