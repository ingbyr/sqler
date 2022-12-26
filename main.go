package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

var (
	flagSqlFile     string
	flagInteractive bool
)

func init() {
	flag.StringVar(&flagSqlFile, "f", "", "sql file path")
	flag.BoolVar(&flagInteractive, "i", true, "interactive mode")
	flag.Parse()
}

func main() {
	cfg := LoadConfig("jdbc.properties")
	sqler := NewSqler(cfg)
	if sqler == nil {
		panic("failed to create sqler")
	}
	if flagSqlFile != "" {
		fmt.Printf("execute sql from file: %s\n", flagSqlFile)
		sqler.Exec(true, LoadSqlFile(flagSqlFile)...)
	}

	if flagInteractive {
		fmt.Println("start mysql shell ...")
		scanner := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("> ")
			line, err := scanner.ReadString('\n')
			if err != nil {
				panic(err)
			}
			line = strings.TrimSpace(line)
			if line == "q" {
				os.Exit(0)
			}
			sqler.Exec(false, line)
		}
	}

}
