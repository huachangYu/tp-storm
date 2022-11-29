package org.apache.storm.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltTask {
    private static final Logger LOG = LoggerFactory.getLogger(BoltTask.class);
    //metrics
    private static Lock lock = new ReentrantLock();

    private final Runnable task;
    private final Tuple tuple;
    private final BoltExecutorMonitor monitor;
    private final boolean recordCost;
    private final String threadName;
    private long createTimeNs;
    private long startTimeNs;
    private long endTimeNs;

    public BoltTask(Runnable task, Tuple tuple, BoltExecutorMonitor monitor, String threadName,
                    boolean recordCost) {
        this.monitor = monitor;
        this.threadName = threadName;
        this.recordCost = recordCost;
        if (shouldRecord()) {
            this.createTimeNs = System.nanoTime();
        }
        this.task = task;
        this.tuple = tuple;
    }

    private boolean shouldRecord() {
        return recordCost;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public long getRootId() {
        return tuple.getRootId();
    }

    public void run() {
        if (shouldRecord()) {
            this.startTimeNs = System.nanoTime();
        }
        task.run();
        if (shouldRecord()) {
            this.endTimeNs = System.nanoTime();
            if (this.endTimeNs > 0) {
                monitor.recordBoltTaskInfo(getWaitingNs(), getCostNs());
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
}
