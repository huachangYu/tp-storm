package org.apache.storm.executor.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.storm.executor.BoltTask;
import org.apache.storm.executor.TaskQueue;
import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public class ScheduleStrategyUtils {
    public static ScheduleStrategy getStrategy(String strategyName,
                                               Lock lock,
                                               Condition emptyQueueWait,
                                               AtomicInteger blockedConsumerNum,
                                               List<TaskQueue> taskQueues) {
        StrategyType strategy = StrategyType.valueOf(strategyName);
        if (strategy == StrategyType.RANDOM) {
            return new RandomStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        } else if (strategy == StrategyType.ROUND_ROBIN) {
            return new RoundRobinStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        } else if (strategy == StrategyType.HT) {
            return new HtStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        } else if (strategy == StrategyType.LL) {
            return new LlStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        } else if (strategy == StrategyType.AD) {
            return new AdStrategy(lock, emptyQueueWait, blockedConsumerNum, taskQueues);
        } else {
            throw new IllegalArgumentException("unknown strategy");
        }
    }

    public static int check(ResizableBlockingQueue<BoltTask> queue0,
                            ResizableBlockingQueue<BoltTask> queue1,
                            BoltExecutorMonitor monitor0,
                            BoltExecutorMonitor monitor1,
                            long currentNs) {
        int remainCapacity0 = queue0.remainingCapacity();
        int remainCapacity1 = queue1.remainingCapacity();
        if (remainCapacity0 <= 0 && remainCapacity1 <= 0) {
            return 0;
        } else if (remainCapacity0 <= 0) {
            return 1;
        } else if (remainCapacity1 <= 0) {
            return -1;
        }

        long waiting0 = monitor0.getWaitingTime(currentNs);
        long waiting1 = monitor1.getWaitingTime(currentNs);
        final long delta = 50 * 1000 * 1000; //50ms
        if (waiting0 >= delta && waiting1 >= delta) {
            return 0;
        } else if (waiting0 >= delta) {
            return 1;
        } else if (waiting1 >= delta) {
            return -1;
        }
        return 2;
    }
}
