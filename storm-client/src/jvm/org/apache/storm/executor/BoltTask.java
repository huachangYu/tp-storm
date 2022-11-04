package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltTask {
    private static final Logger LOG = LoggerFactory.getLogger(BoltTask.class);
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

    public long getCreateTimeNs() {
        return createTimeNs;
    }

    public long getStartTimeNs() {
        return startTimeNs;
    }

    public long getEndTimeNs() {
        return endTimeNs;
    }

    public String getThreadName() {
        return threadName;
    }

    public void printMetrics() {
        if (recordCost) {
            LOG.info("[boltTask] threadName={}, waitingTimeNs={}, costTimeNs={}",
                    threadName, getWaitingNs(), getCostNs());
        }
    }
}
