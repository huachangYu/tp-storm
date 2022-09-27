package org.apache.storm.executor.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.apache.storm.executor.BoltTask;
import org.apache.storm.utils.ConfigUtils;

public class BoltExecutorMonitor {
    private static class BoltTaskInfo {
        long currentTime;
        int currentQueueSize;

        BoltTaskInfo(long currentTime, int currentQueueSize) {
            this.currentTime = currentTime;
            this.currentQueueSize = currentQueueSize;
        }
    }

    private long lastTime = System.currentTimeMillis();
    private int windowsSize = 50;
    private final ReentrantLock taskLock = new ReentrantLock();
    private long timeSpan = 10 * 1000 * 10; // 10s
    private int tasksMaxSize = 1000;
    private final List<BoltTaskInfo> currentTaskInfos = new LinkedList<>();
    private final ReentrantLock costLock = new ReentrantLock();
    private final Queue<Long> costTimeQueue = new LinkedList<>();
    private final AtomicLong totalTime = new AtomicLong(0);
    private double weight = 0.0;
    private String strategy = BoltWeightCalc.Strategy.QueueAndCost.name();
    protected BooleanSupplier costSampler = ConfigUtils.evenSampler(10);
    protected BooleanSupplier predictSampler = ConfigUtils.evenSampler(100);

    public void recordTaskInfo(BoltTask task, int queueSize) {
        taskLock.lock();
        try {
            while (!currentTaskInfos.isEmpty()
                    && (currentTaskInfos.size() >= tasksMaxSize
                    || currentTaskInfos.get(0).currentTime < task.getStartTime() - timeSpan)) {
                currentTaskInfos.remove(0);
            }
            currentTaskInfos.add(new BoltTaskInfo(task.getEndTime(), queueSize));
        } finally {
            taskLock.unlock();
        }
    }

    public void recordCost(long cost) {
        if (cost < 0) {
            cost = 0;
        }
        costLock.lock();
        try {
            if (costTimeQueue.size() >= windowsSize) {
                totalTime.addAndGet(-costTimeQueue.poll());
            }
            costTimeQueue.add(cost);
            totalTime.addAndGet(cost);
        } finally {
            costLock.unlock();
        }
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public void recordLastTime(long ms) {
        this.lastTime = ms;
    }

    public long getLastTime() {
        return lastTime;
    }

    public long getWaitingTime(long current) {
        return current - lastTime;
    }

    public double getAvgTime() {
        int size;
        if ((size = costTimeQueue.size()) == 0) {
            return 0;
        }
        // it is thread-unsafe, but has little effect. To improve performance, don't lock it
        return (double) totalTime.get() / (double) size;
    }
    
    public double calculateWeight(long current, int taskQueueSize, int minTaskQueueSize, int maxTaskQueueSize,
                                double minAvgTime, double maxAvgTime,
                                long minWeightTime, long maxWeightTime) {
        if (strategy == null
                || strategy.length() == 0
                || strategy.equals(BoltWeightCalc.Strategy.Fair.name())) {
            this.weight = BoltWeightCalc.fair(taskQueueSize, getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.OnlyQueue.name())) {
            this.weight = BoltWeightCalc.onlyQueue(taskQueueSize, getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndCost.name())) {
            this.weight = BoltWeightCalc.queueAndCost(taskQueueSize, getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndWait.name())) {
            this.weight = BoltWeightCalc.queueAndWait(taskQueueSize, getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndCostAndWait.name())) {
            this.weight = BoltWeightCalc.queueAndCostAndWait(taskQueueSize, getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else {
            this.weight = 0;
        }
        return this.weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getWeight() {
        return weight;
    }

    public boolean shouldRecordCost() {
        return costSampler.getAsBoolean();
    }

    public boolean shouldRecordTaskInfo() {
        return predictSampler.getAsBoolean();
    }
}
