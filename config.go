package main

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	DataSourceArg string             `yaml:"dataSourceArg"`
	DataSources   []DataSourceConfig `yaml:"dataSources"`
}

type DataSourceConfig struct {
	DataSourceBase  interface{} `yaml:"dataSourceBase,omitempty"`
	Type            string      `yaml:"type"`
	URL             string      `yaml:"url"`
	Schema          string      `yaml:"schema"`
	Username        string      `yaml:"username"`
	Password        string      `yaml:"password"`
	DataSource00001 interface{} `yaml:"dataSource00001,omitempty"`
	DataSource00002 interface{} `yaml:"dataSource00002,omitempty"`
}

func LoadConfig(configFile string) (*Config, error) {
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
