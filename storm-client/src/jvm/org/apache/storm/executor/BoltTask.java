package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltTask {
    private static final Logger LOG = LoggerFactory.getLogger(BoltTask.class);
    private Runnable runnable;
    private final BoltExecutorMonitor monitor;
    private final boolean recordCost;
    private final boolean recordTaskInfo;
    private final String threadName;
    private long enqueueTime;
    private long startTime;
    private long endTime;

    private BoltTask(BoltExecutorMonitor monitor, String threadName, boolean recordCost, boolean recordTaskInfo) {
        this.monitor = monitor;
        this.threadName = threadName;
        this.recordCost = recordCost;
        this.recordTaskInfo = recordTaskInfo;
        if (shouldRecord()) {
            this.enqueueTime = System.currentTimeMillis();
        }
    }

    public BoltTask(Runnable runnable, BoltExecutorMonitor monitor, String threadName,
                    boolean needToRecord, boolean recordTaskInfo) {
        this(monitor, threadName, needToRecord, recordTaskInfo);
        this.runnable = runnable;
    }

    private boolean shouldRecord() {
        return recordCost || recordTaskInfo;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public void run() {
        if (shouldRecord()) {
            this.startTime = System.currentTimeMillis();
        }
        runnable.run();
        if (shouldRecord()) {
            this.endTime = System.currentTimeMillis();
        }
    }

    public boolean shouldRecordCost() {
        return recordCost;
    }

    public boolean shouldRecordTaskInfo() {
        return recordTaskInfo;
    }

    public long getCost() {
        return endTime - startTime;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getThreadName() {
        return threadName;
    }

    public void printMetrics() {
        if (recordCost) {
            LOG.info("[boltTask] threadName={}, waitingTime={}, costTime={}", threadName, startTime - enqueueTime, endTime - startTime);
        }
    }
}
