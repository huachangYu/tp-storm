package org.apache.storm.executor.strategy;

import org.apache.storm.executor.BoltTask;
import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableBlockingQueue;

public class ScheduleStrategyUtils {
    public static ScheduleStrategy getStrategy(String strategyName) {
        StrategyType strategy = StrategyType.valueOf(strategyName);
        if (strategy == StrategyType.Fair) {
            return new FairStrategy();
        } else if (strategy == StrategyType.OnlyQueue) {
            return new OnlyQueueStrategy();
        } else if (strategy == StrategyType.QueueAndCost) {
            return new QueueAndCostStrategy();
        } else if (strategy == StrategyType.QueueAndWait) {
            return new QueueAndWaitStrategy();
        } else if (strategy == StrategyType.QueueAndCostAndWait) {
            return new QueueAndCostAndWaitStrategy();
        } else if (strategy == StrategyType.AD) {
            return new AdStrategy();
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

    public static int fair(ResizableBlockingQueue<BoltTask> queue0,
                           ResizableBlockingQueue<BoltTask> queue1,
                           BoltExecutorMonitor monitor0,
                           BoltExecutorMonitor monitor1,
                           long currentNs) {
        return 0;
    }

    // if onlyQueue(qa, qb, ma, mb) > 0, queue a has higher priority
    public static int onlyQueue(ResizableBlockingQueue<BoltTask> queue0,
                                ResizableBlockingQueue<BoltTask> queue1,
                                BoltExecutorMonitor monitor0,
                                BoltExecutorMonitor monitor1,
                                long currentNs) {
        int diff = queue1.remainingCapacity() - queue0.remainingCapacity();
        if (diff == 0) {
            return 0;
        }
        return diff > 0 ? 1 : -1;
    }

    public static int queueAndCost(ResizableBlockingQueue<BoltTask> queue0,
                                   ResizableBlockingQueue<BoltTask> queue1,
                                   BoltExecutorMonitor monitor0,
                                   BoltExecutorMonitor monitor1,
                                   long currentNs) {
        double cost0 = monitor0.getAvgTimeNs();
        double cost1 = monitor1.getAvgTimeNs();
        int remainCapacity0 = queue0.remainingCapacity();
        int remainCapacity1 = queue1.remainingCapacity();
        double delta = remainCapacity1 * cost1 -  remainCapacity0 * cost0;
        if (delta == 0) {
            return 0;
        }
        return delta > 0 ? 1 : -1;
    }

    public static int queueAndWait(ResizableBlockingQueue<BoltTask> queue0,
                                   ResizableBlockingQueue<BoltTask> queue1,
                                   BoltExecutorMonitor monitor0,
                                   BoltExecutorMonitor monitor1,
                                   long currentNs) {
        int size0 = queue0.size();
        int size1 = queue1.size();
        long waiting0 = monitor0.getWaitingTime(currentNs);
        long waiting1 = monitor1.getWaitingTime(currentNs);
        double delta = ((double) size0 / (double) queue0.getCapacity()) * (double) waiting0
                - ((double) size1 / (double) queue1.getCapacity()) * (double) waiting1;
        if (delta == 0) {
            return 0;
        }
        return delta > 0 ? 1 : -1;
    }

    public static int queueAndCostAndWait(ResizableBlockingQueue<BoltTask> queue0,
                                          ResizableBlockingQueue<BoltTask> queue1,
                                          BoltExecutorMonitor monitor0,
                                          BoltExecutorMonitor monitor1,
                                          long currentNs) {
        double waiting0 = monitor0.getWaitingTime(currentNs);
        double waiting1 = monitor1.getWaitingTime(currentNs);
        double cost0 = monitor0.getAvgTimeNs();
        double cost1 = monitor1.getAvgTimeNs();
        int size0 = queue0.size();
        int size1 = queue1.size();
        double rr0 = size0 * (1 + waiting0 / Math.max(cost0, 1));
        double rr1 = size1 * (1 + waiting1 / Math.max(cost1, 1));
        double delta = rr0 - rr1;
        if (delta == 0) {
            return 0;
        }
        return delta > 0 ? 1 : -1;
    }
}
