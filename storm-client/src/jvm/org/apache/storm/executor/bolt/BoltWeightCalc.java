package org.apache.storm.executor.bolt;

import org.apache.storm.utils.JCQueue;

public class BoltWeightCalc {
    public enum Strategy {
        Fair, OnlyQueue, QueueAndCost, QueueAndWait, QueueAndCostAndWait
    }

    public static double fair(int taskQueueSize, double avgTime, long waitingTime,
                              int minTaskQueueSize, int maxTaskQueueSize,
                              double minAvgTime, double maxAvgTime,
                              long minWaitingTime, long maxWaitingTime) {
        return 1.0;
    }

    public static double onlyQueue(int taskQueueSize, double avgTime, long waitingTime,
                                   int minTaskQueueSize, int maxTaskQueueSize,
                                   double minAvgTime, double maxAvgTime,
                                   long minWaitingTime, long maxWaitingTime) {
        if (waitingTime > 5000) {
            return Double.MAX_VALUE;
        }
        return taskQueueSize;
    }

    public static double queueAndCost(int taskQueueSize, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        if (waitingTime > 5000) {
            return Double.MAX_VALUE;
        }
        return taskQueueSize * (1 + avgTime);
    }

    public static double queueAndWait(int taskQueueSize, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        return (double) taskQueueSize / (double) Math.max(maxTaskQueueSize - minTaskQueueSize, 1)
                + (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
    }

    public static double queueAndCostAndWait(int taskQueueSize, double avgTime, long waitingTime,
                                             int minTaskQueueSize, int maxTaskQueueSize,
                                             double minAvgTime, double maxAvgTime,
                                             long minWaitingTime, long maxWaitingTime) {
        return (double) taskQueueSize / (double) Math.max(maxTaskQueueSize - minTaskQueueSize, 1)
                + avgTime / Math.max(maxAvgTime - minAvgTime, 1)
                + (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
    }

}
