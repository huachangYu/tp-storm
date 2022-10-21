package org.apache.storm.daemon.worker;

import java.util.concurrent.locks.ReentrantLock;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class SystemMonitor {
    public static final int CPU_CORE_NUM = Runtime.getRuntime().availableProcessors();
    private final ReentrantLock lock = new ReentrantLock();
    private final SystemInfo systemInfo;
    private final CentralProcessor systemInfoProcessor;
    private long[] oldTicks;
    private long oldTicksTime;
    private double preResult = -1.0;

    public SystemMonitor() {
        this.systemInfo = new SystemInfo();
        this.systemInfoProcessor = this.systemInfo.getHardware().getProcessor();
        this.oldTicks = this.systemInfoProcessor.getSystemCpuLoadTicks();
        this.oldTicksTime = System.currentTimeMillis();
    }

    /**
     * get the average cpu usage between two records. The time interval is bigger than 5s
     * @return cpu usage.
     */
    public double getAvgCpuUsage() {
        lock.lock();
        try {
            long current = System.currentTimeMillis();
            if (current - oldTicksTime >= 5000) { // the time interval must be bigger than 5s
                long[] ticks = this.systemInfoProcessor.getSystemCpuLoadTicks();
                long total = 0;
                for (int i = 0; i < ticks.length; i++) {
                    total += ticks[i] - oldTicks[i];
                }
                long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - oldTicks[CentralProcessor.TickType.IDLE.getIndex()];
                oldTicks = ticks;
                oldTicksTime = current;
                return (preResult = 1 - (double) idle / (double) total);
            }
            return preResult;
        } finally {
            lock.unlock();
        }
    }

    public double getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        double maxMemory = runtime.maxMemory();
        double freeMemory = runtime.freeMemory();
        double totalMemory = runtime.totalMemory();
        return (totalMemory - freeMemory) / maxMemory;
    }
}
