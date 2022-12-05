package org.apache.storm.executor.strategy;

import java.util.List;
import org.apache.storm.executor.TaskQueue;

public interface IScheduleStrategy {
    TaskQueue getTaskQueue();
}
