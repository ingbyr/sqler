package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func NewCountJob(sqler *Sqler, csvFileName string, schemas []string) Job {
	return &CountJob{
		sqler:       sqler,
		csvFileName: csvFileName,
		schemas:     schemas,
		BaseJob:     NewBaseJob(new(JobCtx)),
	}
}

type CountJob struct {
	sqler       *Sqler
	csvFileName string
	schemas     []string
	*BaseJob
}

func (job *CountJob) Exec() {
	// ds - ds - count
	schemaDsCountMap := make(map[string]map[string]string)
	for _, schema := range job.schemas {
		schemaDsCountMap[schema] = make(map[string]string)
	}
	errs := make([]error, 0)
	for dbID, db := range sqler.dbs {
		ds := sqler.cfg.DataSources[dbID]
		for _, schema := range job.schemas {
			cntQuery := "select count(*) from " + schema
			results, err := db.Query(cntQuery)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			_, rows, err := convertSqlResults(results)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			schemaDsCountMap[schema][ds.DsKey()] = rows[0][0]
		}
		fmt.Printf("Count db %d/%d %s\n", dbID+1, len(sqler.dbs), ds.DsKey())
	}

	if len(errs) > 0 {
		for _, err := range errs {
			job.RecordError(err)
		}
		return
	}

	// Csv
	file, err := os.OpenFile(job.csvFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	csvWriter := csv.NewWriter(file)

	header := make([]string, 0, len(job.schemas)+1)
	header = append(header, "Tables")
	for _, ds := range job.sqler.cfg.DataSources {
		header = append(header, ds.DsKey())
	}
	if err := csvWriter.Write(header); err != nil {
		job.RecordError(err)
		return
	}

	for _, schema := range job.schemas {
		tableRow := make([]string, 0, len(sqler.cfg.DataSources)+1)
		tableRow = append(tableRow, schema)
		for _, ds := range sqler.cfg.DataSources {
			tableRow = append(tableRow, schemaDsCountMap[schema][ds.DsKey()])
		}
		// Csv content
		if err := csvWriter.Write(tableRow); err != nil {
			job.RecordError(err)
			return
		}
		csvWriter.Flush()
	}
	fmt.Println("Result saved to " + file.Name())
	return
}
