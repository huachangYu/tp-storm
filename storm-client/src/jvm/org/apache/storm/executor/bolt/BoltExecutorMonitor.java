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
    private long lastTimeNs = System.nanoTime();
    private final int windowsSize = 100;
    private final ReentrantLock costLock = new ReentrantLock();
    private final Queue<Long> costTimeQueue = new LinkedList<>();
    private final AtomicLong totalTime = new AtomicLong(0);
    private final BooleanSupplier costSampler = ConfigUtils.evenSampler(10);

    public BoltExecutorMonitor() {
    }

    public void recordCost(long ns) {
        if (ns < 0) {
            ns = 0;
        }
        costLock.lock();
        try {
            if (costTimeQueue.size() >= windowsSize) {
                totalTime.addAndGet(-costTimeQueue.poll());
            }
            costTimeQueue.add(ns);
            totalTime.addAndGet(ns);
        } finally {
            costLock.unlock();
        }
    }

    public void recordLastTime(long ns) {
        this.lastTimeNs = ns;
    }

    public long getLastTimeNs() {
        return lastTimeNs;
    }

    public long getWaitingTime(long ns) {
        return ns - lastTimeNs;
    }

    public double getAvgTime() {
        int size;
        if ((size = costTimeQueue.size()) == 0) {
            return 0;
        }
        // it is thread-unsafe, but has little effect. To improve performance, don't lock it
        return (double) totalTime.get() / (double) size;
    }

    public boolean shouldRecordCost() {
        return costSampler.getAsBoolean();
    }
}
