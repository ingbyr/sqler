package main

var _ ExecutableJob = (*PrintJob)(nil)

func NewPrintJob(msg string, level Level) Job {
	job := NewJob(level, &PrintJob{
		msg:     msg,
		visible: true,
	})
	job.SetPrintable(true)
	job.Done()
	return job
}

type PrintJob struct {
	msg     string
	visible bool
	*DefaultJob
}

func (p *PrintJob) DoExec() error {
	return nil
}

func (p *PrintJob) Output() []byte {
	return []byte(p.msg)
}

func (p *PrintJob) SetWrapper(job *DefaultJob) {
	p.DefaultJob = job
}
