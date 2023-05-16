package main

var _ ExecutableJob = (*PrintJob)(nil)

func NewSimplePrintJob(msg string, level Level) Job {
	printJob := newPrintJob(msg)
	job := WrapJob(level, printJob)
	job.Done()
	return job
}

func NewNoOutputPrintJob(msg string, level Level) Job {
	printJob := newPrintJob(msg)
	printJob.visible = false
	return WrapJob(level, printJob)
}

func newPrintJob(msg string) *PrintJob {
	return &PrintJob{
		msg:        msg,
		visible:    true,
		DefaultJob: nil,
	}
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
