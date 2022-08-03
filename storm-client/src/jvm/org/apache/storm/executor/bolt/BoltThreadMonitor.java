package org.apache.storm.executor.bolt;

import java.util.LinkedList;
import java.util.Queue;

public class BoltThreadMonitor {
    private int windowsSize = 100;
    private long totalTime = 0;
    private Queue<Long> costTimeQueue = new LinkedList<>();
    private long startTime = 0;
    private long endTime = 0;

    public synchronized void startRecord() {
        startTime = System.currentTimeMillis();
    }

    public synchronized void endRecord() {
        if (startTime <= 0) {
            throw new RuntimeException("Function[startRecord] is needed to be called before Function[endRecordTime]");
        }
        endTime = System.currentTimeMillis();
        long cost = endTime - startTime;
        record(cost);
    }

    public synchronized void record(long cost) {
        if (costTimeQueue.size() == windowsSize) {
            totalTime -= costTimeQueue.poll();
        }
        costTimeQueue.add(cost);
        totalTime += totalTime;
    }

    public synchronized double getAvgTime() {
        if (costTimeQueue.size() == 0) {
            return 0;
        }
        return (double) totalTime / (double) costTimeQueue.size();
    }
}
