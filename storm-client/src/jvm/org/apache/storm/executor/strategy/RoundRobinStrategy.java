package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.storm.executor.TaskQueue;

public class RoundRobinStrategy extends ScheduleStrategy {
    private int currentIndex = 0;

    public RoundRobinStrategy(Lock lock,
                              Condition emptyQueueWait,
                              AtomicInteger blockedConsumerNum,
                              List<TaskQueue> taskQueues) {
        super(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
    }

    @Override
    public TaskQueue getTaskQueue() {
        currentIndex = (currentIndex + 1) % taskQueues.size();
        return taskQueues.get(currentIndex);
    }
}
