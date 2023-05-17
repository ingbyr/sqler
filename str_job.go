package main

var _ ExecutableJob = (*StrJob)(nil)

func NewStrJob(msg string, level Level) Job {
	strJob := &StrJob{
		msg:        msg,
		visible:    true,
		DefaultJob: nil,
	}
	job := WrapJobWithLevel(strJob, level)
	job.output.WriteString(msg)
	job.Done()
	return job
}

type StrJob struct {
	msg     string
	visible bool
	*DefaultJob
}

func (p *StrJob) DoExec() error {
	return nil
}

func (p *StrJob) SetWrapper(job *DefaultJob) {
	p.DefaultJob = job
}
