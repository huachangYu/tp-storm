package org.apache.storm.executor.bolt;

import java.util.LinkedList;
import java.util.Queue;

public class BoltExecutorMonitor {
    private long lastTime = System.currentTimeMillis();
    private int windowsSize = 100;
    private long totalTime = 0;
    private Queue<Long> costTimeQueue = new LinkedList<>();

    public synchronized void record(long cost) {
        if (costTimeQueue.size() == windowsSize) {
            totalTime -= costTimeQueue.poll();
        }
        costTimeQueue.add(cost);
        totalTime += totalTime;
    }

    public void recordLastTime(long ms) {
        this.lastTime = ms;
    }

    public long getLastTime() {
        return lastTime;
    }

    public synchronized double getAvgTime() {
        if (costTimeQueue.size() == 0) {
            return 0;
        }
        return (double) totalTime / (double) costTimeQueue.size();
    }
}
