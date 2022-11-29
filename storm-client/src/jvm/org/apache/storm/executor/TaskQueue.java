package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ResizableBlockingQueue;

public class TaskQueue {
    private final ResizableBlockingQueue<BoltTask> queue;
    private final JCQueue receiveQueue;
    private final BoltExecutorMonitor monitor;

    public TaskQueue(int capacity, JCQueue receiveQueue, BoltExecutorMonitor monitor) {
        this.queue = new ResizableBlockingQueue<>(capacity);
        this.receiveQueue = receiveQueue;
        this.monitor = monitor;
    }

    public ResizableBlockingQueue<BoltTask> getQueue() {
        return queue;
    }

    public BoltExecutorMonitor getMonitor() {
        return monitor;
    }

    public long getFirstRootId() {
        BoltTask task = queue.peek();
        return task == null ? 0 : task.getRootId();
    }

    public int getTotalSize() {
        return queue.size() + receiveQueue.size();
    }

    public long getWaitingTime(long ns) {
        return monitor.getWaitingTime(ns);
    }
}
