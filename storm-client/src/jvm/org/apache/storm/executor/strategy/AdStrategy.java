package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.storm.executor.TaskQueue;

public class AdStrategy extends ScheduleStrategy {

    private HtStrategy htStrategy;
    private LlStrategy llStrategy;

    public AdStrategy(Lock lock,
                      Condition emptyQueueWait,
                      AtomicInteger blockedConsumerNum,
                      List<TaskQueue> taskQueues) {
        super(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        this.htStrategy = new HtStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        this.llStrategy = new LlStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
    }

    @Override
    public TaskQueue getTaskQueue() {
        if (status == 0) {
            return llStrategy.getTaskQueue();
        } else if (status == 1) {
            return htStrategy.getTaskQueue();
        }
        return llStrategy.getTaskQueue();
    }
}
