package org.apache.storm.executor.bolt;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.apache.storm.utils.ConfigUtils;

public class BoltExecutorMonitor {
    private long lastTimeNs = System.nanoTime();
    private final int windowsSize = 100;
    private final ReentrantLock costLock = new ReentrantLock();
    private final String executorName;
    private final Queue<Long> costTimeNsQueue = new LinkedList<>();
    private final AtomicLong totalTimeNs = new AtomicLong(0);
    private final BooleanSupplier costSampler = ConfigUtils.evenSampler(10);

    public BoltExecutorMonitor(String executorName) {
        this.executorName = executorName;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void recordCost(long ns) {
        if (ns < 0) {
            ns = 0;
        }
        costLock.lock();
        try {
            if (costTimeNsQueue.size() >= windowsSize) {
                totalTimeNs.addAndGet(-costTimeNsQueue.poll());
            }
            costTimeNsQueue.add(ns);
            totalTimeNs.addAndGet(ns);
        } finally {
            costLock.unlock();
        }
    }

    public void recordLastTime(long ns) {
        this.lastTimeNs = ns;
    }

    public long getWaitingTime(long ns) {
        return ns - lastTimeNs;
    }

    public double getAvgTimeNs() {
        int size;
        if ((size = costTimeNsQueue.size()) == 0) {
            return 0;
        }
        // it is thread-unsafe, but has little effect. To improve performance, don't lock it
        return (double) totalTimeNs.get() / (double) size;
    }

    public boolean shouldRecordCost() {
        return costSampler.getAsBoolean();
    }
}
