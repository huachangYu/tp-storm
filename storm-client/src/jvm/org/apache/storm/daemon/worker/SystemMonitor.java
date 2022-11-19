package org.apache.storm.daemon.worker;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.daemon.Shutdownable;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class SystemMonitor implements Shutdownable {
    public static final int CPU_CORE_NUM = Runtime.getRuntime().availableProcessors();
    private static final long TIME_SPAN_MS = 1000;
    private final ReentrantLock lock = new ReentrantLock();
    private final CentralProcessor systemInfoProcessor;
    private final Thread cpuUpdater;
    private long[] preTicks;
    private volatile boolean cpuUpdaterRunning;
    private double cpuUsage = 0.0;

    public SystemMonitor() {
        SystemInfo systemInfo = new SystemInfo();
        this.systemInfoProcessor = systemInfo.getHardware().getProcessor();
        this.preTicks = this.systemInfoProcessor.getSystemCpuLoadTicks();
        this.cpuUpdaterRunning = true;
        this.cpuUpdater = new Thread(() -> {
            final int idleIndex = CentralProcessor.TickType.IDLE.getIndex();
            while (cpuUpdaterRunning) {
                try {
                    Thread.sleep(TIME_SPAN_MS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                lock.lock();
                try {
                    long[] ticks = this.systemInfoProcessor.getSystemCpuLoadTicks();
                    long total = 0;
                    for (int i = 0; i < ticks.length; i++) {
                        total += ticks[i] - preTicks[i];
                    }
                    if (total == 0) {
                        continue;
                    }
                    long idle = ticks[idleIndex] - preTicks[idleIndex];
                    cpuUsage = 1 - (double) idle / (double) total;
                    preTicks = ticks;
                } finally {
                    lock.unlock();
                }
            }

        });
        this.cpuUpdater.setDaemon(true);
        this.cpuUpdater.start();
    }

    public double getCpuUsage() {
        lock.lock();
        try {
            return cpuUsage;
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

    @Override
    public void shutdown() {
        if (cpuUpdater != null) {
            this.cpuUpdaterRunning = false;
            cpuUpdater.interrupt();
        }
    }
}
