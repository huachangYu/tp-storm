package org.apache.storm.daemon.worker;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.daemon.Shutdownable;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class SystemMonitor implements Shutdownable {
    public static final int CPU_CORE_NUM;
    private static final long CPU_USAGE_TIME_SPAN_MS = 1000; // 1s
    private static final int CPU_USAGE_LIST_SIZE = 60;

    static {
        SystemInfo systemInfo = new SystemInfo();
        CentralProcessor processor = systemInfo.getHardware().getProcessor();
        CPU_CORE_NUM = processor.getLogicalProcessorCount();
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final CentralProcessor systemInfoProcessor;
    private final Thread cpuUpdater;
    private final Queue<Double> cpuUsageList;
    private long[] preTicks;
    private volatile boolean cpuUpdaterRunning;
    private double cpuUsage = 0.0;

    public SystemMonitor() {
        SystemInfo systemInfo = new SystemInfo();
        this.systemInfoProcessor = systemInfo.getHardware().getProcessor();
        this.preTicks = this.systemInfoProcessor.getSystemCpuLoadTicks();
        this.cpuUsageList = new ArrayDeque<>(CPU_USAGE_LIST_SIZE);
        this.cpuUpdaterRunning = true;
        this.cpuUpdater = getCpuUpdater();
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

    public double getAverageCpuUsage(int seconds) {
        if (seconds <= 0) {
            throw new IllegalArgumentException("second must be bigger than 0");
        }
        lock.lock();
        try {
            Double[] cpuUsages = cpuUsageList.toArray(new Double[0]);
            double total = 0.0;
            int len = cpuUsages.length;
            int n = Math.min(seconds, len);
            for (int i = 0; i < n; i++) {
                total += cpuUsages[len - 1 - i];
            }
            return total / seconds;
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

    private Thread getCpuUpdater() {
        return new Thread(() -> {
            final int idleIndex = CentralProcessor.TickType.IDLE.getIndex();
            while (cpuUpdaterRunning) {
                try {
                    Thread.sleep(CPU_USAGE_TIME_SPAN_MS);
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
                    if (cpuUsageList.size() >= CPU_USAGE_LIST_SIZE) {
                        cpuUsageList.poll();
                    }
                    cpuUsageList.add(cpuUsage);
                    preTicks = ticks;
                } finally {
                    lock.unlock();
                }
            }
        });
    }
}
