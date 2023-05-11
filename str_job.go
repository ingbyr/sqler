package main

type StrPrintJob struct {
	msg     string
	visible bool
	*DefaultPrintJob
}

func NewStrPrintJob(msg string, level Level) *StrPrintJob {
	job := &StrPrintJob{
		msg:             msg,
		visible:         true,
		DefaultPrintJob: NewDefaultPrintJob(level),
	}
	job.printJob = job
	return job
}

func (p *StrPrintJob) Msg() []byte {
	return []byte(p.msg)
}
