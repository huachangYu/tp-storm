package org.apache.storm.executor.bolt;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class BoltExecutorMonitor {
    private long lastTime = System.currentTimeMillis();
    private int windowsSize = 50;
    private BlockingQueue<Long> costTimeQueue = new LinkedBlockingQueue<>();
    private AtomicLong totalTime = new AtomicLong(0);
    private ReentrantLock lock = new ReentrantLock();
    private double weight = 0.0;
    private String strategy;

    public void record(long cost) {
        if (cost < 0) {
            cost = 0;
        }
        lock.lock();
        try {
            if (costTimeQueue.size() >= windowsSize) {
                totalTime.addAndGet(-costTimeQueue.poll());
            }
            costTimeQueue.add(cost);
            totalTime.addAndGet(cost);
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

    @Override
    public String toString() {
        return "BoltExecutorMonitor{"
                + "lastTime=" + lastTime
                + ", windowsSize=" + windowsSize
                + ", costTimeQueue=" + costTimeQueue
                + ", lock=" + lock
                + ", weight=" + weight
                + ", strategy='" + strategy + '\''
                + '}';
    }
}
