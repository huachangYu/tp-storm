package org.apache.storm.executor;

import java.util.concurrent.FutureTask;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;

public class BoltTask {
    private Runnable runnable;
    private FutureTask<?> future;
    private BoltExecutorMonitor monitor;
    private boolean needToRecord;

    public BoltTask(Runnable runnable, BoltExecutorMonitor monitor, boolean needToRecord) {
        this.runnable = runnable;
        this.monitor = monitor;
        this.needToRecord = needToRecord;
    }

    public BoltTask(FutureTask<?> future, BoltExecutorMonitor monitor, boolean needToRecord) {
        this.future = future;
        this.monitor = monitor;
        this.needToRecord = needToRecord;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public void run() {
        if (runnable != null) {
            runnable.run();
        } else if (future != null) {
            future.run();
        }
    }

    public boolean isNeedToRecord() {
        return needToRecord;
    }
}
