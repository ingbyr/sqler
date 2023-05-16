package pkg

import (
	"gopkg.in/yaml.v3"
	"os"
)

const DefaultDataSourceArgs = "collation=utf8mb4_general_ci&multiStatements=true&multiStatements=true"

type Config struct {
	FileName       string              `yaml:"-"`
	DataSourceArgs string              `yaml:"dataSourceArgs"`
	DataSources    []*DataSourceConfig `yaml:"dataSources"`
}

type DataSourceConfig struct {
	Type     string `yaml:"type"`
	Url      string `yaml:"url"`
	Schema   string `yaml:"schema"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Enabled  bool   `yaml:"enabled"`
}

func NewConfig() *Config {
	return &Config{
		DataSourceArgs: DefaultDataSourceArgs,
		DataSources:    make([]*DataSourceConfig, 0),
	}
}

func LoadConfigFromFile(configFile string) (*Config, error) {
	file, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	cfg := new(Config)
	if err = yaml.Unmarshal(file, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (cfg *Config) AddDataSource(ds *DataSourceConfig) {
	cfg.DataSources = append(cfg.DataSources, ds)
}
