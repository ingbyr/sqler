package pkg

import (
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

const DefaultDataSourceArgs = "collation=utf8mb4_general_ci&multiStatements=true&multiStatements=true"

func NewConfig() *Config {
	return &Config{
		DataSourceArgs: DefaultDataSourceArgs,
		DataSources:    make([]*DataSourceConfig, 0),
		CommandsConfig: &CommandsConfig{CountSchemas: make([]string, 0)},
	}
}

func LoadConfigFromFile(configFile string, aes *AesCipher) (*Config, error) {
	file, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	cfg := new(Config)
	if err = yaml.Unmarshal(file, cfg); err != nil {
		return nil, err
	}
	if aes != nil {
		cfg.decryptProperties(aes)
	}
	return cfg, nil
}

type Config struct {
	FileName       string              `yaml:"-"`
	DataSourceArgs string              `yaml:"dataSourceArgs"`
	DataSources    []*DataSourceConfig `yaml:"dataSources"`
	CommandsConfig *CommandsConfig     `yaml:"commands"`
}

func (cfg *Config) AddDataSource(ds *DataSourceConfig) {
	cfg.DataSources = append(cfg.DataSources, ds)
}

func (cfg *Config) decryptProperties(aes *AesCipher) {
	prefix := "ENC("
	suffix := ")"
	for _, ds := range cfg.DataSources {
		if strings.HasPrefix(ds.Username, prefix) && strings.HasSuffix(ds.Username, suffix) {
			ds.Password = aes.DecAsStr(ds.Username[len(prefix) : len(ds.Username)-len(suffix)])
		}
		if strings.HasPrefix(ds.Password, prefix) && strings.HasSuffix(ds.Password, suffix) {
			ds.Password = aes.DecAsStr(ds.Password[len(prefix) : len(ds.Password)-len(suffix)])
		}
	}
}

type DataSourceConfig struct {
	Type     string `yaml:"type"`
	Url      string `yaml:"url"`
	Schema   string `yaml:"schema"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Enabled  bool   `yaml:"enabled"`
}

func (ds *DataSourceConfig) DsKey() string {
	return ds.Url + "/" + ds.Schema
}

type CommandsConfig struct {
	CountSchemas []string `yaml:"count-schemas"`
}

func (c *CommandsConfig) AddCountSchema(schema string) {
	c.CountSchemas = append(c.CountSchemas, schema)
}
