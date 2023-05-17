package main

import (
	"github.com/olekukonko/tablewriter"
)

var _ ExecutableJob = (*CountJob)(nil)

func NewCountJob(sqler *Sqler, schemas []string) Job {
	return WrapJob(&CountJob{
		sqler:   sqler,
		schemas: schemas,
	})
}

type CountJob struct {
	sqler   *Sqler
	schemas []string
	*DefaultJob
}

func (c *CountJob) DoExec() error {
	// schema - ds - count
	schemaDsCountMap := make(map[string]map[string]string)
	for _, schema := range c.schemas {
		schemaDsCountMap[schema] = make(map[string]string)
	}
	for _, schema := range c.schemas {
		for dbID, db := range sqler.dbs {
			cntQuery := "select count(*) from " + schema
			results, err := db.Query(cntQuery)
			if err != nil {
				return err
			}
			_, rows, err := convertSqlResults(results)
			if err != nil {
				return err
			}
			schemaDsCountMap[schema][sqler.cfg.DataSources[dbID].DsKey()] = rows[0][0]
		}
	}

	// Write output
	table := tablewriter.NewWriter(c.output)
	header := make([]string, 0, len(c.schemas)+1)
	header = append(header, "DataSource")
	for _, schema := range c.schemas {
		header = append(header, schema)
	}
	table.SetHeader(header)
	for _, ds := range sqler.cfg.DataSources {
		tableRow := make([]string, 0, len(c.schemas)+1)
		tableRow = append(tableRow, ds.DsKey())
		for _, schema := range c.schemas {
			tableRow = append(tableRow, schemaDsCountMap[schema][ds.DsKey()])
		}
		table.Append(tableRow)
	}
	table.Render()
	return nil
}

func (c *CountJob) SetWrapper(defaultJob *DefaultJob) {
	c.DefaultJob = defaultJob
}
