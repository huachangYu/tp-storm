package org.apache.storm.executor.bolt;

public class BoltWeightCalc {
    public enum Strategy {
        Fair, OnlyQueue, QueueAndCost, QueueAndWait, QueueAndCostAndWait
    }

    private static final double EPS = 1e-5;
    private static final int INF = 1000000000;

    public static double fair(int queueSize, int queueCapacity, double avgTime, long waitingTime,
                              int minTaskQueueSize, int maxTaskQueueSize,
                              double minAvgTime, double maxAvgTime,
                              long minWaitingTime, long maxWaitingTime) {
        return 1.0;
    }

    public static double onlyQueue(int queueSize, int queueCapacity, double avgTime, long waitingTime,
                                   int minTaskQueueSize, int maxTaskQueueSize,
                                   double minAvgTime, double maxAvgTime,
                                   long minWaitingTime, long maxWaitingTime) {
        return INF - (queueCapacity - queueSize);
    }

    public static double queueAndCost(int queueSize, int queueCapacity, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        return INF - (queueCapacity - queueSize) * (avgTime < EPS ? 1.0 : avgTime);
    }

    public static double queueAndWait(int queueSize, int queueCapacity, double avgTime, long waitingTime,
                                      int minTaskQueueSize, int maxTaskQueueSize,
                                      double minAvgTime, double maxAvgTime,
                                      long minWaitingTime, long maxWaitingTime) {
        double stdWaitingTime = (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
        return queueSize * stdWaitingTime;
    }

    public static double queueAndCostAndWait(int queueSize, int queueCapacity, double avgTime, long waitingTime,
                                             int minTaskQueueSize, int maxTaskQueueSize,
                                             double minAvgTime, double maxAvgTime,
                                             long minWaitingTime, long maxWaitingTime) {
        double stdWaitingTime = (double) waitingTime / (double) Math.max(maxWaitingTime - minWaitingTime, 1);
        double stdAvgTime = avgTime / Math.max(maxAvgTime - minAvgTime, 1);
        return queueSize * (1 + stdWaitingTime / Math.max(stdAvgTime, 0.001));
    }

}
