package org.apache.storm.executor.bolt;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.storm.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltExecutorMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutorMonitor.class);
    private static final int COST_WINDOW_SIZE = 100;
    private static final int METRICS_WINDOW_SIZE = 30;
    private static final long PERIOD_TIME_INTERVAL = 1000; // 1000ms

    private final String executorName;
    private long lastProcessTimeNs = System.nanoTime();
    private final Lock costLock = new ReentrantLock();
    private final Queue<Long> costTimeWindowNs = new ArrayDeque<>(COST_WINDOW_SIZE);
    private final AtomicLong totalCostTimeNs = new AtomicLong(0);
    private final BooleanSupplier costSampler = ConfigUtils.evenSampler(10);

    private final Lock metricsLock = new ReentrantLock();
    private final boolean shouldStartMetricsThread;
    private final boolean printMetrics;
    private Thread metricsThread;
    private long totalWaitingTimeNsPeriod = 0;
    private long totalProcessTimeNsPeriod = 0;
    private long totalCountPeriod = 0;
    private final Deque<Double> waitingTimePeriodWindowNs = new ArrayDeque<>(METRICS_WINDOW_SIZE);
    private final Deque<Double> processTimePeriodWindowNs = new ArrayDeque<>(METRICS_WINDOW_SIZE);

    public BoltExecutorMonitor(String executorName, boolean executorPoolOptimize, boolean printMetrics) {
        this.executorName = executorName;
        this.printMetrics = printMetrics;
        this.shouldStartMetricsThread = executorPoolOptimize;
        if (this.shouldStartMetricsThread) {
            startMetricsThread();
        }
    }

    public String getExecutorName() {
        return executorName;
    }

    public void recordCost(long ns) {
        if (ns < 0) {
            ns = 0;
        }
        costLock.lock();
        try {
            if (costTimeWindowNs.size() >= COST_WINDOW_SIZE) {
                totalCostTimeNs.addAndGet(-costTimeWindowNs.poll());
            }
            costTimeWindowNs.add(ns);
            totalCostTimeNs.addAndGet(ns);
        } finally {
            costLock.unlock();
        }
    }

    public void recordLastTime(long ns) {
        this.lastProcessTimeNs = ns;
    }

    public long getWaitingTime(long ns) {
        return ns - lastProcessTimeNs;
    }

    public double getAvgTimeNs() {
        int size;
        if ((size = costTimeWindowNs.size()) == 0) {
            return 0;
        }
        // it is thread-unsafe, but has little effect. To improve performance, don't lock it
        return (double) totalCostTimeNs.get() / (double) size;
    }

    public void recordBoltTaskInfo(long waitingTimeNs, long processTimeNs) {
        if (!shouldStartMetricsThread) {
            return;
        }
        metricsLock.lock();
        try {
            totalWaitingTimeNsPeriod += waitingTimeNs;
            totalProcessTimeNsPeriod += processTimeNs;
            totalCountPeriod++;
        } finally {
            metricsLock.unlock();
        }
    }

    public void clearPeriodWindow() {
        metricsLock.lock();
        try {
            waitingTimePeriodWindowNs.clear();
            processTimePeriodWindowNs.clear();
        } finally {
            metricsLock.unlock();
        }
    }

    public boolean isFlowSurge() {
        metricsLock.lock();
        try {
            if (waitingTimePeriodWindowNs.size() < METRICS_WINDOW_SIZE) {
                return false;
            }
            Double[] waitingTimes = waitingTimePeriodWindowNs.toArray(new Double[0]);
            double firstPartTotalTimeNs = 0;
            double secondPartTotalTimeNs = 0;
            int mid = waitingTimes.length / 2;
            for (int i = 0; i < mid; i++) {
                firstPartTotalTimeNs += waitingTimes[i];
            }
            for (int i = mid; i < waitingTimes.length; i++) {
                secondPartTotalTimeNs += waitingTimes[i];
            }
            // average waiting time > 5ms and data input rate increased rapidly
            return secondPartTotalTimeNs > 5000 * 1000 * (waitingTimes.length - mid)
                    && 3 * firstPartTotalTimeNs < secondPartTotalTimeNs;
        } finally {
            metricsLock.unlock();
        }
    }

    private void startMetricsThread() {
        this.metricsThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(PERIOD_TIME_INTERVAL);

                    metricsLock.lock();

                    if (totalCountPeriod == 0) {
                        if (waitingTimePeriodWindowNs.size() > 0) {
                            waitingTimePeriodWindowNs.addLast(waitingTimePeriodWindowNs.getLast());
                        }
                        if (processTimePeriodWindowNs.size() > 0) {
                            processTimePeriodWindowNs.addLast(processTimePeriodWindowNs.getLast());
                        }
                        continue;
                    }

                    double averageWaitingTimeNs = (double) totalWaitingTimeNsPeriod / (double) totalCountPeriod;
                    double averageProcessTimeNs = (double) totalProcessTimeNsPeriod / (double) totalCountPeriod;
                    if (printMetrics) {
                        LOG.info("[BoltExecutorMonitor] threadName={}, averageWaitingTime={}ms, averageCostTime={}ms",
                                executorName, String.format("%.4f", averageWaitingTimeNs / (1000 * 1000)),
                                String.format("%.4f", averageProcessTimeNs / (1000 * 1000)));
                    }

                    while (waitingTimePeriodWindowNs.size() >= METRICS_WINDOW_SIZE) {
                        waitingTimePeriodWindowNs.poll();
                    }
                    waitingTimePeriodWindowNs.add(averageWaitingTimeNs);
                    while (processTimePeriodWindowNs.size() >= METRICS_WINDOW_SIZE) {
                        processTimePeriodWindowNs.poll();
                    }
                    processTimePeriodWindowNs.add(averageProcessTimeNs);

                    totalWaitingTimeNsPeriod = 0;
                    totalProcessTimeNsPeriod = 0;
                    totalCountPeriod = 0;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    metricsLock.unlock();
                }
            }
        });
        this.metricsThread.setDaemon(true);
        this.metricsThread.start();
    }

    public boolean shouldRecordCost() {
        return costSampler.getAsBoolean();
    }
}
