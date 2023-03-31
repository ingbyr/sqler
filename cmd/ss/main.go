package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"regexp"
	"sqler/pkg"
	"strings"
)

var (
	Version   string
	BuildTime string
	BuildBy   string
)

func main() {
	args := os.Args
	if len(args) < 3 {
		fmt.Println("sqler-ss [source config yaml] [target config yaml]")
		printInfo()
		os.Exit(1)
	}
	ssCfgFileName := args[1]
	cfgFileName := args[2]
	ssCfgFile, err := os.ReadFile(ssCfgFileName)
	panicIfError(err)
	ssCfg := new(ShardingSphereConfig)
	err = yaml.Unmarshal(ssCfgFile, ssCfg)
	if err != nil {
		panic(err)
	}

	cfg := &pkg.Config{
		DataSourceArgs: pkg.DefaultDataSourceArgs,
		DataSources:    make([]pkg.DataSourceConfig, 0, len(ssCfg.DataSources)),
	}
	for _, ssDs := range ssCfg.DataSources {
		// FIXME Support general config
		url, schema := parseJdbcUrl(ssDs["jdbcUrl"])
		cfg.AddDataSource(pkg.DataSourceConfig{
			Type:     pkg.DsTypeMysql,
			Url:      url,
			Schema:   schema,
			Username: ssDs["username"],
			Password: ssDs["password"],
		})
	}
	out, err := yaml.Marshal(cfg)
	panicIfError(err)
	err = os.WriteFile(cfgFileName, out, 0775)
	panicIfError(err)
	fmt.Println("Generated file", cfgFileName)
}

type ShardingSphereConfig struct {
	DataSources map[string]map[string]string `yaml:"dataSources"`
}

func parseJdbcUrl(jdbcUrl string) (string, string) {
	regstr := `\d+\.\d+\.\d+\.\d+:\d+/.+\?`
	reg, _ := regexp.Compile(regstr)
	res := reg.FindString(jdbcUrl)
	split := strings.Split(res, "/")
	return split[0], split[1][:len(split[1])-1]
}

func panicIfError(err error) {
	if err != nil {
		printInfo()
		panic(err)
	}
}

func printInfo() {
	fmt.Println("Version:", Version)
	fmt.Println("BuildTime:", BuildTime)
	fmt.Println("BuildBy:", BuildBy)
}
