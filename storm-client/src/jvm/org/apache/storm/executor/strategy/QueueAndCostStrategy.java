package org.apache.storm.executor.strategy;

import org.apache.storm.executor.BoltTask;
import org.apache.storm.executor.TaskQueue;
import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public class QueueAndCostStrategy extends ScheduleStrategy {
    @Override
    public int compare(TaskQueue taskQueue0, TaskQueue taskQueue1, long currentNs) {
        ResizableBlockingQueue<BoltTask> queue0 = taskQueue0.getQueue();
        BoltExecutorMonitor monitor0 = taskQueue0.getMonitor();
        ResizableBlockingQueue<BoltTask> queue1 = taskQueue1.getQueue();
        BoltExecutorMonitor monitor1 = taskQueue0.getMonitor();
        int check = ScheduleStrategyUtils.check(queue0, queue1, monitor0, monitor1, currentNs);
        if (check != 2) {
            return check;
        }
        return ScheduleStrategyUtils.queueAndCost(queue0, queue1, monitor0, monitor1, currentNs);
    }

}
