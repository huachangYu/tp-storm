package org.apache.storm.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltTask {
    private static final Logger LOG = LoggerFactory.getLogger(BoltTask.class);
    //metrics
    private static Lock lock = new ReentrantLock();
    private static boolean enablePrintMetrics = false;
    private static Map<String, Long> totalWaiting = new HashMap<>();
    private static Map<String, Long> totalCost = new HashMap<>();
    private static Map<String, Long> lastPrintTimeNs = new HashMap<>();
    private static Map<String, Long> totalCount = new HashMap<>();

    private Runnable task;
    private final BoltExecutorMonitor monitor;
    private final boolean recordCost;
    private final String threadName;
    private long createTimeNs;
    private long startTimeNs;
    private long endTimeNs;

    private BoltTask(BoltExecutorMonitor monitor, String threadName, boolean recordCost) {
        this.monitor = monitor;
        this.threadName = threadName;
        this.recordCost = recordCost;
        if (shouldRecord()) {
            this.createTimeNs = System.nanoTime();
        }
    }

    public BoltTask(Runnable task, BoltExecutorMonitor monitor, String threadName,
                    boolean needToRecord) {
        this(monitor, threadName, needToRecord);
        this.task = task;
    }

    public static void setEnablePrintMetrics(boolean enablePrintMetrics) {
        BoltTask.enablePrintMetrics = enablePrintMetrics;
    }

    private boolean shouldRecord() {
        return recordCost;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public void run() {
        if (shouldRecord()) {
            this.startTimeNs = System.nanoTime();
        }
        task.run();
        if (shouldRecord()) {
            this.endTimeNs = System.nanoTime();
            if (enablePrintMetrics) {
                updateMetrics();
            }
        }
    }

    public boolean shouldRecordCost() {
        return recordCost;
    }

    public long getCostNs() {
        return endTimeNs - startTimeNs;
    }

    public long getWaitingNs() {
        return startTimeNs - createTimeNs;
    }

    public void updateMetrics() {
        if (endTimeNs == 0 || !shouldRecord()) {
            return;
        }
        lock.lock();
        if (!lastPrintTimeNs.containsKey(threadName)) {
            lastPrintTimeNs.put(threadName, endTimeNs);
        }
        Long lastTimeNs = lastPrintTimeNs.get(threadName);
        if (endTimeNs - lastTimeNs >= 1000 * 1000 * 1000) {
            // print metrics per second
            long waitingTimeNs = totalWaiting.getOrDefault(threadName, 0L);
            long costTimeNs = totalCost.getOrDefault(threadName, 0L);
            long count = totalCount.getOrDefault(threadName, 1L);
            LOG.info("[boltTask] threadName={}, averageWaitingTime={}ms, averageCostTime={}ms",
                    threadName,
                    String.format("%.4f", (double) waitingTimeNs / (double) (1000 * 1000 * count)),
                    String.format("%.4f", (double) costTimeNs / (double) (1000 * 1000 * count)));
            lastPrintTimeNs.put(threadName, endTimeNs);
            totalCost.put(threadName, 0L);
            totalWaiting.put(threadName, 0L);
            totalCount.put(threadName, 0L);
        } else {
            totalCost.put(threadName, totalCost.getOrDefault(threadName, 0L) + getCostNs());
            totalWaiting.put(threadName, totalWaiting.getOrDefault(threadName, 0L) + getWaitingNs());
            totalCount.put(threadName, totalCount.getOrDefault(threadName, 0L) + 1);
        }
        lock.unlock();
    }
}
