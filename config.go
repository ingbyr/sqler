package main

import (
	"bufio"
	"os"
	"strings"
)

type Config struct {
	DataSources   []*DataSource
	DataSourceArg string
}

type DataSource struct {
	Url      string
	Schema   string
	Username string
	Password string
}

func NewDataSource() *DataSource {
	return &DataSource{}
}

func (ds *DataSource) Loaded() bool {
	return ds.Url != "" && ds.Username != "" && ds.Password != ""
}

func LoadConfig(dataSourceFile string) *Config {
	file, err := os.Open(dataSourceFile)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	cfg := &Config{
		DataSources:   make([]*DataSource, 0),
		DataSourceArg: "collation=utf8mb4_general_ci&multiStatements=true&multiStatements=true",
	}
	ds := NewDataSource()
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.Split(line, "=")
		if len(kv) < 2 {
			panic("can not parse: '" + line + "'")
		}
		k := strings.TrimSpace(kv[0])
		v := strings.Join(kv[1:], "=")

		if strings.HasSuffix(k, "url") {
			vs := strings.Split(strings.TrimPrefix(v, "jdbc:mysql://"), "/")
			ds.Url = vs[0]
			ds.Schema = strings.Split(vs[1], "?")[0]
		} else if strings.HasSuffix(k, "username") {
			ds.Username = v
		} else if strings.HasSuffix(k, "password") {
			ds.Password = v
		}

		if ds.Loaded() {
			cfg.DataSources = append(cfg.DataSources, ds)
			ds = NewDataSource()
		}
	}
	return cfg
}
