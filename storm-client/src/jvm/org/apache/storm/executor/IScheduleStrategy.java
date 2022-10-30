package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public interface IScheduleStrategy {
    int compare(ResizableBlockingQueue<BoltTask> queue0,
                ResizableBlockingQueue<BoltTask> queue1,
                BoltExecutorMonitor monitor0,
                BoltExecutorMonitor monitor1,
                long currentNs);
}
