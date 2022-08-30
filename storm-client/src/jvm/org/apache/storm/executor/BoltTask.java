package org.apache.storm.executor;

import java.util.concurrent.FutureTask;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;

public class BoltTask {
    private FutureTask<?> task;
    private BoltExecutorMonitor monitor;
    private boolean needToRecord;

    public BoltTask(FutureTask<?> task, BoltExecutorMonitor monitor, boolean needToRecord) {
        this.task = task;
        this.monitor = monitor;
        this.needToRecord = needToRecord;
    }

    public FutureTask<?> getTask() {
        return task;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public boolean isNeedToRecord() {
        return needToRecord;
    }
}
