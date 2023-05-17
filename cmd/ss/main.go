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
	sourceCfgFileName := args[1]
	targetFileName := args[2]

	ssCfg := loadSsCfg(sourceCfgFileName)
	cfg := transToCfg(ssCfg)

	cfgBytes, err := yaml.Marshal(cfg)
	panicIfError(err)
	err = os.WriteFile(targetFileName, cfgBytes, 0775)
	panicIfError(err)

	fmt.Println("Generated file", targetFileName)
}

func transToCfg(ssCfg *ShardingSphereConfig) *pkg.Config {
	cfg := pkg.NewConfig()
	transDataSourcesConfig(cfg, ssCfg.DataSources.Content)
	transCountConfig(cfg, ssCfg.Rules.Content)
	return cfg
}

func transDataSourcesConfig(cfg *pkg.Config, dataSources []*yaml.Node) {
	for i := 0; i < len(dataSources); i += 2 {
		//dsKey := dataSources[i]
		dsContent := dataSources[i+1]
		ds := map[string]string{}
		if err := dsContent.Decode(ds); err != nil {
			panic(err)
		}
		url, schema := parseJdbcUrl(ds["jdbcUrl"])
		cfg.AddDataSource(&pkg.DataSourceConfig{
			Type:     pkg.DsTypeMysql,
			Url:      url,
			Schema:   schema,
			Username: ds["username"],
			Password: ds["password"],
			Enabled:  true,
		})
	}
}

func transCountConfig(cfg *pkg.Config, rules []*yaml.Node) {
	for _, rule := range rules {
		if rule.Tag == "!SHARDING" {
			for i, sharding := range rule.Content {
				if sharding.Value == "broadcastTables" {
					schemas := rule.Content[i+1]
					for _, schema := range schemas.Content {
						cfg.CommandsConfig.AddCountSchema(schema.Value)
					}
					return
				}
			}
		}
	}
}

func loadSsCfg(ssCfgFileName string) *ShardingSphereConfig {
	ssCfgFile, err := os.ReadFile(ssCfgFileName)
	panicIfError(err)
	ssCfg := new(ShardingSphereConfig)
	err = yaml.Unmarshal(ssCfgFile, ssCfg)
	panicIfError(err)
	return ssCfg
}

type ShardingSphereConfig struct {
	//DataSources map[string]map[string]string `yaml:"dataSources"`
	FileName    string    `yaml:"-"`
	DataSources yaml.Node `yaml:"dataSources"`
	Rules       yaml.Node `yaml:"rules"`
}

func parseJdbcUrl(jdbcUrl string) (string, string) {
	pattern := `\d+\.\d+\.\d+\.\d+:\d+/.+\?`
	reg, _ := regexp.Compile(pattern)
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
