package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
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
	// ds - ds - count
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
			fmt.Printf("Count db %s\n", ds.DsKey())
			wg.Done()
		}(dbID, db, sqler.cfg.DataSources[dbID])
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Csv
	file, err := os.OpenFile("count.csv", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	csvWriter := csv.NewWriter(file)

	header := make([]string, 0, len(c.schemas)+1)
	header = append(header, "Tables")
	for _, ds := range c.sqler.cfg.DataSources {
		header = append(header, ds.DsKey())
	}
	if err := csvWriter.Write(header); err != nil {
		return err
	}

	for _, schema := range c.schemas {
		tableRow := make([]string, 0, len(sqler.cfg.DataSources)+1)
		tableRow = append(tableRow, schema)
		for _, ds := range sqler.cfg.DataSources {
			tableRow = append(tableRow, schemaDsCountMap[schema][ds.DsKey()])
		}
		// Csv content
		if err := csvWriter.Write(tableRow); err != nil {
			return err
		}
		csvWriter.Flush()
	}
	fmt.Println("Result saved to " + file.Name())
	return nil
}

func (c *CountJob) SetWrapper(defaultJob *DefaultJob) {
	c.DefaultJob = defaultJob
}
