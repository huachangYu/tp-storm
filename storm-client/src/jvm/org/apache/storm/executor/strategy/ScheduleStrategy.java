package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.apache.storm.executor.TaskQueue;

public abstract class ScheduleStrategy implements IScheduleStrategy {
    protected static final Random RAND = new Random();
    protected final Lock lock;
    protected final Condition emptyQueueWait;
    protected final AtomicInteger blockedConsumerNum;
    protected final List<TaskQueue> taskQueues;
    protected int status = 0; // 0: low load, 1: high load

    public ScheduleStrategy(Lock lock,
                            Condition emptyQueueWait,
                            AtomicInteger blockedConsumerNum,
                            List<TaskQueue> taskQueues) {
        this.lock = lock;
        this.emptyQueueWait = emptyQueueWait;
        this.blockedConsumerNum = blockedConsumerNum;
        this.taskQueues = taskQueues;
    }

    protected List<TaskQueue> getNotEmptyQueue() {
        lock.lock();
        try {
            List<TaskQueue> notEmptyTaskQueues = taskQueues.stream()
                    .filter(t -> t.getQueue().size() > 0)
                    .collect(Collectors.toList());
            while (notEmptyTaskQueues.isEmpty()) {
                blockedConsumerNum.getAndIncrement();
                emptyQueueWait.await();
                blockedConsumerNum.getAndDecrement();
                notEmptyTaskQueues = taskQueues.stream()
                        .filter(t -> t.getQueue().size() > 0)
                        .collect(Collectors.toList());
            }
            return notEmptyTaskQueues;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
