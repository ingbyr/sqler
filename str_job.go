package main

var _ ExecutableJob = (*StrJob)(nil)

func NewStrJob(msg string, level Level) Job {
	printJob := newStrJob(msg)
	job := WrapJobWithLevel(printJob, level)
	job.Done()
	return job
}

func NewNoOutputStrJob(msg string, level Level) Job {
	printJob := newStrJob(msg)
	printJob.visible = false
	return WrapJobWithLevel(printJob, level)
}

func newStrJob(msg string) *StrJob {
	return &StrJob{
		msg:        msg,
		visible:    true,
		DefaultJob: nil,
	}
}

type StrJob struct {
	msg     string
	visible bool
	*DefaultJob
}

func (p *StrJob) DoExec() error {
	return nil
}

func (p *StrJob) Output() []byte {
	return []byte(p.msg)
}

func (p *StrJob) SetWrapper(job *DefaultJob) {
	p.DefaultJob = job
}
