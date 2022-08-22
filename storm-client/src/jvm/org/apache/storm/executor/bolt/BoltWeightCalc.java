package org.apache.storm.executor.bolt;

import org.apache.storm.utils.JCQueue;

public class BoltWeightCalc {
    public enum Strategy {
        Fair, OnlyQueue, QueueAndCost, QueueAndWait, QueueAndCostAndWait
    }

    public static double fair(JCQueue receiveQueue, BoltExecutorMonitor monitor, long current) {
        return 1.0;
    }

    public static double onlyQueue(JCQueue receiveQueue, BoltExecutorMonitor monitor, long current) {
        return receiveQueue.size();
    }

    public static double queueAndCost(JCQueue receiveQueue, BoltExecutorMonitor monitor, long current) {
        return receiveQueue.size() * (1 + monitor.getAvgTime());
    }

    public static double queueAndWait(JCQueue receiveQueue, BoltExecutorMonitor monitor, long current) {
        return receiveQueue.size() * (1 + current - monitor.getLastTime());
    }

    public static double queueAndCostAndWait(JCQueue receiveQueue, BoltExecutorMonitor monitor, long current) {
        return receiveQueue.size() * (1 + monitor.getAvgTime()) * (1 + current - monitor.getLastTime());
    }

}
