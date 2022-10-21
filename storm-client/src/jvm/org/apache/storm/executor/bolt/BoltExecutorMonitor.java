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
    public static class BoltTaskInfo {
        long currentTimeNs;
        int currentQueueSize;

        public long getCurrentTimeNs() {
            return currentTimeNs;
        }

        public int getCurrentQueueSize() {
            return currentQueueSize;
        }

        BoltTaskInfo(long currentTimeNs, int currentQueueSize) {
            this.currentTimeNs = currentTimeNs;
            this.currentQueueSize = currentQueueSize;
        }
    }

    private long lastTimeNs = System.nanoTime();
    private final int windowsSize = 100;
    private final ReentrantLock taskInfoLock = new ReentrantLock();
    private final long timeSpanNs = 100L * 1000 * 10 * 1000 * 1000; // 100s
    private final int tasksMaxSize = 1000;
    private final List<BoltTaskInfo> currentTaskInfos = new LinkedList<>();
    private final ReentrantLock costLock = new ReentrantLock();
    private final Queue<Long> costTimeQueue = new LinkedList<>();
    private final AtomicLong totalTime = new AtomicLong(0);
    private final BooleanSupplier costSampler = ConfigUtils.evenSampler(10);
    private final BooleanSupplier predictSampler = ConfigUtils.evenSampler(100);

    public BoltExecutorMonitor() {
    }

    public void recordTaskInfo(BoltTask task, int queueSize) {
        taskInfoLock.lock();
        try {
            while (!currentTaskInfos.isEmpty()
                    && (currentTaskInfos.size() >= tasksMaxSize
                    || currentTaskInfos.get(0).currentTimeNs < task.getStartTimeNs() - timeSpanNs)) {
                currentTaskInfos.remove(0);
            }
            BoltTaskInfo boltTaskInfo = new BoltTaskInfo(System.nanoTime(), queueSize);
            if (currentTaskInfos.isEmpty()
                    || currentTaskInfos.get(currentTaskInfos.size() - 1).getCurrentTimeNs() < boltTaskInfo.getCurrentTimeNs()) {
                currentTaskInfos.add(boltTaskInfo);
            }
        } finally {
            taskInfoLock.unlock();
        }
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

    public List<BoltTaskInfo> getCurrentTaskInfos(long currentNs) {
        taskInfoLock.lock();
        try {
            while (!currentTaskInfos.isEmpty()
                    && (currentTaskInfos.size() >= tasksMaxSize
                    || currentTaskInfos.get(0).currentTimeNs < currentNs - timeSpanNs)) {
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
