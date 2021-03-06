package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var waitAll sync.WaitGroup
	for task:=0; task < ntasks; task++ {
		waitAll.Add(1)
		go func(taskNum int, nios int, phase jobPhase){
			defer waitAll.Done()
			for{
				worker := <-mr.registerChannel
				var taskArgs DoTaskArgs
				taskArgs.JobName = mr.jobName
				taskArgs.File = mr.files[taskNum]
				taskArgs.Phase = phase
				taskArgs.TaskNumber = taskNum
				taskArgs.NumOtherPhase = nios
		
				ok := call(worker, "Worker.DoTask", &taskArgs, new(struct{}))
				if ok {
						go func(){
							mr.registerChannel <- worker
						}()
					break;
				}
			    }

		}(task, nios, phase)
											
	}
	waitAll.Wait()
}
