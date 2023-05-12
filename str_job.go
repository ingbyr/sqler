package main

var _ ExecutableJob = (*PrintJob)(nil)

type PrintJob struct {
	msg     string
	visible bool
	*DefaultJob
}

func (p *PrintJob) DoExec() error {
	return nil
}

func (p *PrintJob) SetWrapper(job *DefaultJob) {
	p.DefaultJob = job
}

func NewPrintJob(msg string, level Level) Job {
	printJob := &PrintJob{
		msg:     msg,
		visible: true,
	}
	printJob.SetPrintable(true)

	return NewJob(level, printJob)
}

func (p *PrintJob) Output() []byte {
	return []byte(p.msg)
}
