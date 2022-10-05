package org.apache.storm.executor.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.BoltTask;
import org.apache.storm.utils.ConfigUtils;

public class BoltExecutorMonitor {
    public static class BoltTaskInfo {
        long currentTime;
        int currentQueueSize;

        public long getCurrentTime() {
            return currentTime;
        }

        public int getCurrentQueueSize() {
            return currentQueueSize;
        }

        BoltTaskInfo(long currentTime, int currentQueueSize) {
            this.currentTime = currentTime;
            this.currentQueueSize = currentQueueSize;
        }
    }

    private final WorkerState workerData;
    private long lastTime = System.currentTimeMillis();
    private int windowsSize = 50;
    private final ReentrantLock taskInfoLock = new ReentrantLock();
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

    public BoltExecutorMonitor(WorkerState workerData) {
        this.workerData = workerData;
    }

    public void recordTaskInfo(BoltTask task, int queueSize) {
        taskInfoLock.lock();
        try {
            while (!currentTaskInfos.isEmpty()
                    && (currentTaskInfos.size() >= tasksMaxSize
                    || currentTaskInfos.get(0).currentTime < task.getStartTime() - timeSpan)) {
                currentTaskInfos.remove(0);
            }
            BoltTaskInfo boltTaskInfo = new BoltTaskInfo(System.currentTimeMillis(), queueSize);
            if (currentTaskInfos.isEmpty()
                    || currentTaskInfos.get(currentTaskInfos.size() - 1).getCurrentTime() < boltTaskInfo.getCurrentTime()) {
                currentTaskInfos.add(boltTaskInfo);
            }
        } finally {
            taskInfoLock.unlock();
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
    
    public double updateWeight(long current, int queueSize, int queueCapacity,
                               int minTaskQueueSize, int maxTaskQueueSize,
                               double minAvgTime, double maxAvgTime,
                               long minWeightTime, long maxWeightTime) {
        if (strategy == null
                || strategy.length() == 0
                || strategy.equals(BoltWeightCalc.Strategy.Fair.name())) {
            this.weight = BoltWeightCalc.fair(queueSize, queueCapacity,
                    getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.OnlyQueue.name())) {
            this.weight = BoltWeightCalc.onlyQueue(queueSize, queueCapacity,
                    getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndCost.name())) {
            this.weight = BoltWeightCalc.queueAndCost(queueSize, queueCapacity,
                    getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndWait.name())) {
            this.weight = BoltWeightCalc.queueAndWait(queueSize, queueCapacity,
                    getAvgTime(), getWaitingTime(current),
                    minTaskQueueSize, maxTaskQueueSize,
                    minAvgTime, maxAvgTime,
                    minWeightTime, maxWeightTime);
        } else if (strategy.equals(BoltWeightCalc.Strategy.QueueAndCostAndWait.name())) {
            this.weight = BoltWeightCalc.queueAndCostAndWait(queueSize, queueCapacity,
                    getAvgTime(), getWaitingTime(current),
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

    public List<BoltTaskInfo> getCurrentTaskInfos(long current) {
        taskInfoLock.lock();
        try {
            while (!currentTaskInfos.isEmpty()
                    && (currentTaskInfos.size() >= tasksMaxSize
                    || currentTaskInfos.get(0).currentTime < current - timeSpan)) {
                currentTaskInfos.remove(0);
            }
            return currentTaskInfos;
        } finally {
            taskInfoLock.unlock();
        }
    }

    public boolean shouldRecordCost() {
        return costSampler.getAsBoolean();
    }

    public boolean shouldRecordTaskInfo() {
        return predictSampler.getAsBoolean();
    }
}
