package org.apache.storm.executor;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.utils.ResizableBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskQueueOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueOptimizer.class);
    private int minCapacity;
    private int totalCapacity;
    private double overloadThreshold;
    private double lowloadThreshold;
    private double expandThreshold;
    private double reduceThreshold;
    private ConcurrentHashMap<String, ResizableBlockingQueue<BoltTask>> taskQueues;

    public TaskQueueOptimizer(ConcurrentHashMap<String, ResizableBlockingQueue<BoltTask>> taskQueues,
                              int minCapacity,
                              int totalCapacity,
                              double overloadThreshold,
                              double lowloadThreshold,
                              double expandThreshold,
                              double reduceThreshold) {
        this.taskQueues = taskQueues;
        this.minCapacity = minCapacity;
        this.totalCapacity = totalCapacity;
        this.overloadThreshold = overloadThreshold;
        this.lowloadThreshold = lowloadThreshold;
        this.expandThreshold = expandThreshold;
        this.reduceThreshold = reduceThreshold;
    }

    public int getRemainCapacity() {
        return totalCapacity - taskQueues.values().stream()
                .mapToInt(ResizableBlockingQueue::getCapacity).sum();
    }

    public boolean expandIfNeeded(String queueName) {
        ResizableBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
        if (queue == null) {
            return false;
        }
        int size = queue.size();
        int capacity = queue.getCapacity();
        if (size < overloadThreshold * capacity) {
            return false;
        }
        int remainCapacity = getRemainCapacity();
        if (remainCapacity <= 0) {
            return false;
        }
        int increase = Math.min(remainCapacity, (int) (expandThreshold * capacity));
        queue.resizeQueue(capacity + increase);
        return true;
    }

    public boolean reduceIfNeeded(String queueName) {
        ResizableBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
        if (queue == null) {
            return false;
        }
        int size = queue.size();
        int capacity = queue.getCapacity();
        if (capacity <= minCapacity || size >= lowloadThreshold * capacity) {
            return false;
        }
        int decrease = Math.min(capacity - minCapacity, (int) (reduceThreshold * capacity));
        queue.resizeQueue(capacity - decrease);
        return true;
    }

    public void optimize() {
        boolean printInfo = false;
        for (String queueName : taskQueues.keySet()) {
            if (expandIfNeeded(queueName) || reduceIfNeeded(queueName)) {
                printInfo = true;
            }
        }
        if (printInfo) {
            LOG.info("optimize queue size. queue info: {}.",
                    Arrays.toString(taskQueues.entrySet().stream()
                            .map(t -> String.format("\"%s\":(%d/%d)",
                                    t.getKey(), t.getValue().size(), t.getValue().getCapacity())).toArray()));
        }
    }
}
