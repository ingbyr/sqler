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
	flagSqlFile     string
	flagInteractive bool
	flagParallel    bool
)

func parseFlags() {
	flag.StringVar(&flagSqlFile, "f", "", "(file) sql文件路径")
	flag.BoolVar(&flagInteractive, "i", false, "(interactive) 交互模式")
	flag.BoolVar(&flagParallel, "p", false, "(parallel) 并行执行sql")
	flag.Parse()

	if flagSqlFile == "" && !flagInteractive {
		flag.PrintDefaults()
		os.Exit(0)
	}
}

var quit chan os.Signal

func main() {
	parseFlags()
	quit = make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill)

	cfg := LoadConfig("jdbc.properties")
	sqler := NewSqler(cfg)
	if sqler == nil {
		panic("failed to create sqler")
	}
	if flagSqlFile != "" {
		fmt.Printf("execute sql from file: %s\n", flagSqlFile)
		if flagParallel {
			sqler.ExecInParallel(true, LoadSqlFile(flagSqlFile)...)
		} else {
			sqler.Exec(true, LoadSqlFile(flagSqlFile)...)
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
				if flagParallel {
					sqler.ExecInParallel(false, line)
				} else {
					sqler.Exec(false, line)
				}
			}
		}
	}

}
