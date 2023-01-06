package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
)

var (
	flagSqlFile     string
	flagInteractive bool
	flagParallel    bool
)

func init() {
	flag.StringVar(&flagSqlFile, "f", "", "sql文件路径")
	flag.BoolVar(&flagInteractive, "i", false, "交互模式")
	flag.BoolVar(&flagParallel, "p", false, "并行执行sql")
	flag.Parse()
}

var quit chan os.Signal

func main() {

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
					panic(err)
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
