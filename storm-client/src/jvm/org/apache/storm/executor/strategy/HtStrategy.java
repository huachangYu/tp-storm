package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.storm.executor.TaskQueue;

public class HtStrategy extends ScheduleStrategy {

    public HtStrategy(Lock lock,
                      Condition emptyQueueWait,
                      AtomicInteger blockedConsumerNum,
                      List<TaskQueue> taskQueues) {
        super(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
    }

    public int compare(TaskQueue taskQueue0, TaskQueue taskQueue1) {
        long diff = taskQueue1.getQueue().remainingCapacity() - taskQueue0.getQueue().remainingCapacity();
        if (diff == 0) {
            return 0;
        }
        return diff > 0 ? 1 : -1;
    }

    @Override
    public TaskQueue getTaskQueue() {
        List<TaskQueue> notEmptyQueue = getNotEmptyQueue();
        int threadsNum = notEmptyQueue.size();
        int startIndex = 0;
        if (threadsNum > 1) {
            startIndex = RAND.nextInt(threadsNum);
        }
        TaskQueue taskQueue = taskQueues.get(startIndex);
        for (int i = 0; i < threadsNum; i++) {
            if (i == startIndex) {
                continue;
            }
            TaskQueue tmpTaskQueue = taskQueues.get(i);
            if (compare(taskQueue, tmpTaskQueue) < 0) {
                taskQueue = tmpTaskQueue;
            }
        }
        return taskQueue;
    }
}
