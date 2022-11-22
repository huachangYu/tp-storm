package org.apache.storm.executor.strategy;

import org.apache.storm.executor.TaskQueue;
import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public interface IScheduleStrategy {
    int compare(TaskQueue taskQueue0, TaskQueue taskQueue1, long currentNs);
}
