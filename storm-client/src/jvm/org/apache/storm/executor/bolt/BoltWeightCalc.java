package org.apache.storm.executor.bolt;

public class BoltWeightCalc {
    public enum Strategy {
        Fair, OnlyQueue, QueueAndCost, QueueAndWait, QueueAndCostAndWait
    }

    private static final double EPS = 1e-5;

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
        return taskQueueSize;
    }

    public static double queueAndCost(int taskQueueSize, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        return taskQueueSize * (avgTime < EPS ? 1.0 : avgTime);
    }

    public static double queueAndWait(int taskQueueSize, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        double stdWaitingTime = (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
        return taskQueueSize * stdWaitingTime;
    }

    public static double queueAndCostAndWait(int taskQueueSize, double avgTime, long waitingTime,
                                             int minTaskQueueSize, int maxTaskQueueSize,
                                             double minAvgTime, double maxAvgTime,
                                             long minWaitingTime, long maxWaitingTime) {
        double stdWaitingTime = (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
        double stdAvgTime = avgTime / Math.max(maxAvgTime - minAvgTime, 1);
        return taskQueueSize * (1 + stdWaitingTime / Math.max(stdAvgTime, 0.001));
    }

}
