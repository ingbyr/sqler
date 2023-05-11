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

	cfg := pkg.NewConfig()
	dataSources := ssCfg.DataSources.Content
	for i := 0; i < len(dataSources); i += 2 {
		//dsKey := dataSources[i]
		dsContent := dataSources[i+1]
		ds := map[string]string{}
		if err := dsContent.Decode(ds); err != nil {
			panic(err)
		}
		url, schema := parseJdbcUrl(ds["jdbcUrl"])
		cfg.AddDataSource(pkg.DataSourceConfig{
			Type:     pkg.DsTypeMysql,
			Url:      url,
			Schema:   schema,
			Username: ds["username"],
			Password: ds["password"],
			Enabled:  true,
		})
	}
	out, err := yaml.Marshal(cfg)
	panicIfError(err)
	err = os.WriteFile(cfgFileName, out, 0775)
	panicIfError(err)
	fmt.Println("Generated file", cfgFileName)
}

type ShardingSphereConfig struct {
	//DataSources map[string]map[string]string `yaml:"dataSources"`
	DataSources yaml.Node `yaml:"dataSources"`
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
