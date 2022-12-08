package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.storm.executor.TaskQueue;

public class RandomStrategy extends ScheduleStrategy {

    public RandomStrategy(Lock lock,
                          Condition emptyQueueWait,
                          AtomicInteger blockedConsumerNum,
                          List<TaskQueue> taskQueues) {
        super(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
    }

    @Override
    public TaskQueue getTaskQueue() {
        List<TaskQueue> notEmptyQueue = getNotEmptyQueue();
        int threadsNum = notEmptyQueue.size();
        int startIndex = 0;
        if (threadsNum > 1) {
            startIndex = RAND.nextInt(threadsNum);
        }
        return notEmptyQueue.get(startIndex);
    }
}
