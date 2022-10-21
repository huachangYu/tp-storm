package org.apache.storm.executor;

import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableLinkedBlockingQueue;

public interface IScheduleStrategy {
    int compare(ResizableLinkedBlockingQueue<BoltTask> queue0,
                ResizableLinkedBlockingQueue<BoltTask> queue1,
                BoltExecutorMonitor monitor0,
                BoltExecutorMonitor monitor1,
                long currentNs);
}
