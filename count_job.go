package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"github.com/olekukonko/tablewriter"
	"os"
	"sqler/pkg"
	"sync"
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
	mapMu := new(sync.Mutex)
	schemaDsCountMap := make(map[string]map[string]string)
	for _, schema := range c.schemas {
		schemaDsCountMap[schema] = make(map[string]string)
	}
	errsMu := new(sync.Mutex)
	errs := make([]error, 0)
	wg := new(sync.WaitGroup)
	wg.Add(len(c.sqler.dbs))
	for dbID, db := range sqler.dbs {
		go func(dbID int, db *sql.DB, ds *pkg.DataSourceConfig) {
			for _, schema := range c.schemas {
				cntQuery := "select count(*) from " + schema
				results, err := db.Query(cntQuery)
				if err != nil {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
					continue
				}
				_, rows, err := convertSqlResults(results)
				if err != nil {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
					continue
				}
				mapMu.Lock()
				schemaDsCountMap[schema][ds.DsKey()] = rows[0][0]
				mapMu.Unlock()
			}
			wg.Done()
		}(dbID, db, sqler.cfg.DataSources[dbID])
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Write output
	table := tablewriter.NewWriter(c.output)

	// Csv
	file, err := os.OpenFile("count.csv", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	csvWriter := csv.NewWriter(file)

	header := make([]string, 0, len(c.schemas)+1)
	header = append(header, "DataSource")
	for _, schema := range c.schemas {
		header = append(header, schema)
	}
	// Table header
	table.SetHeader(header)
	// Csv header
	if err := csvWriter.Write(header); err != nil {
		return err
	}

	for _, ds := range sqler.cfg.DataSources {
		tableRow := make([]string, 0, len(c.schemas)+1)
		tableRow = append(tableRow, ds.DsKey())
		for _, schema := range c.schemas {
			tableRow = append(tableRow, schemaDsCountMap[schema][ds.DsKey()])
		}
		// Table content
		table.Append(tableRow)
		// Csv content
		if err := csvWriter.Write(tableRow); err != nil {
			return err
		}
	}
	table.Render()
	csvWriter.Flush()
	return nil
}

func (c *CountJob) SetWrapper(defaultJob *DefaultJob) {
	c.DefaultJob = defaultJob
}
