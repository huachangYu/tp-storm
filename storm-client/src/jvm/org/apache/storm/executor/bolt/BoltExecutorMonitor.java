package org.apache.storm.executor.bolt;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class BoltExecutorMonitor {
    private long lastTime = System.currentTimeMillis();
    private int windowsSize = 100;
    private long totalTime = 0;
    private ConcurrentLinkedQueue<Long> costTimeQueue = new ConcurrentLinkedQueue<>();
    private ReentrantLock lock = new ReentrantLock();
    private double weight = 0.0;
    
    private String strategy;

    public void record(long cost) {
        lock.lock();
        try {
            if (costTimeQueue.size() >= windowsSize) {
                totalTime -= costTimeQueue.poll();
            }
            costTimeQueue.add(cost);
            totalTime += totalTime;
        } finally {
            lock.unlock();
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
        lock.lock();
        try {
            if (costTimeQueue.size() == 0) {
                return 0;
            }
            return (double) totalTime / (double) costTimeQueue.size();
        } finally {
            lock.unlock();
        }
    }
    
    public void calculateWeight(long current, int taskQueueSize, int minTaskQueueSize, int maxTaskQueueSize,
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
    }

    public double getWeight() {
        return weight;
    }
}
