package org.apache.storm.executor;

import java.util.concurrent.FutureTask;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltTask {
    private static final Logger LOG = LoggerFactory.getLogger(BoltTask.class);
    private Runnable runnable;
    private FutureTask<?> future;
    private BoltExecutorMonitor monitor;
    private boolean needToRecord;
    private String threadName;
    private long enqueueTime;
    private long startTime;
    private long endTime;

    private BoltTask(BoltExecutorMonitor monitor, boolean needToRecord) {
        this.monitor = monitor;
        this.needToRecord = needToRecord;
        if (needToRecord) {
            this.enqueueTime = System.currentTimeMillis();
        }
    }

    public BoltTask(Runnable runnable, BoltExecutorMonitor monitor, boolean needToRecord) {
        this(monitor, needToRecord);
        this.runnable = runnable;
    }

    public BoltTask(FutureTask<?> future, BoltExecutorMonitor monitor, boolean needToRecord) {
        this(monitor, needToRecord);
        this.future = future;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public void run() {
        if (needToRecord) {
            this.startTime = System.currentTimeMillis();
        }
        if (runnable != null) {
            runnable.run();
        } else if (future != null) {
            future.run();
        }
        if (needToRecord) {
            this.endTime = System.currentTimeMillis();
        }
    }

    public boolean isNeedToRecord() {
        return needToRecord;
    }

    public long getCost() {
        return endTime - startTime;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public void printMetrics() {
        if (needToRecord) {
            LOG.info("[boltTask] boltName={}, waitingTime={}, costTime={}", threadName, startTime - enqueueTime, endTime - startTime);
        }
    }
}
