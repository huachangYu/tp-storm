package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public class TaskQueue {
    private final ResizableBlockingQueue<BoltTask> queue;
    private final BoltExecutorMonitor monitor;

    public TaskQueue(int capacity, BoltExecutorMonitor monitor) {
        this.queue = new ResizableBlockingQueue<>(capacity);
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
}
