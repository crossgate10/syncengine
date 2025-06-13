package syncengine

type DefaultExecutor struct{}

func (d *DefaultExecutor) Submit(task func()) {
	go task()
}
