package org.apache.storm.executor.strategy;

import org.apache.storm.executor.TaskQueue;

public class AdStrategy extends ScheduleStrategy {
    public static  class LowLatencyStrategy extends ScheduleStrategy {
        @Override
        public int compare(TaskQueue taskQueue0, TaskQueue taskQueue1, long currentNs) {
            long diff = taskQueue1.getFirstRootId() - taskQueue0.getFirstRootId();
            if (diff == 0) {
                return 0;
            }
            return diff > 0 ? 1 : -1;
        }
    }

    public static class HighThroughputStrategy extends ScheduleStrategy {
        @Override
        public int compare(TaskQueue taskQueue0, TaskQueue taskQueue1, long currentNs) {
            long diff = taskQueue1.getQueue().remainingCapacity() - taskQueue0.getQueue().remainingCapacity();
            if (diff == 0) {
                return 0;
            }
            return diff > 0 ? 1 : -1;
        }
    }

    private HighThroughputStrategy highThroughputStrategy = new HighThroughputStrategy();
    private LowLatencyStrategy lowLatencyStrategy = new LowLatencyStrategy();

    @Override
    public int compare(TaskQueue taskQueue0, TaskQueue taskQueue1, long currentNs) {
        if (status == 0) {
            return lowLatencyStrategy.compare(taskQueue0, taskQueue1, currentNs);
        } else if (status == 1) {
            return highThroughputStrategy.compare(taskQueue0, taskQueue1, currentNs);
        }
        return 0;
    }
}
