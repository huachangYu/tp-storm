package org.apache.storm.daemon;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerOptimizer.class);
    private final String topologyName;
    private final Map<String, Object> topologyConf;
    private final int maxWorkers;
    private final long timeInterval = 10;
    private final long optimizeTimeInterval = 120 * 1000; //TODO change it to 2 minutes
    private final long sampleTimes = optimizeTimeInterval / timeInterval;
    private long lastOptimizeTime = System.currentTimeMillis();
    private final Queue<Long> overloadTimes = new LinkedList<>();
    private final OverLoadChecker checker;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public WorkerOptimizer(String topologyName, Map<String, Object> topologyConf, int maxWorkers, OverLoadChecker checker) {
        this.topologyName = topologyName;
        this.topologyConf = topologyConf;
        this.maxWorkers = maxWorkers;
        this.checker = checker;
    }

    private void optimize() {
        long workerNum = (Long) topologyConf.getOrDefault(Config.TOPOLOGY_WORKERS, 1);
        if (workerNum >= maxWorkers) {
            return;
        }
        final int newWorkerNum = (int) workerNum + 1;
        final RebalanceOptions rebalanceOptions = new RebalanceOptions();
        rebalanceOptions.set_num_workers(newWorkerNum);
        LOG.info("Set number of workers of topology {} to {}", topologyName, newWorkerNum);
        try {
            NimbusClient.withConfiguredClient(nimbus -> {
                nimbus.rebalance(topologyName, rebalanceOptions);
                LOG.info("Topology {} is rebalancing", topologyName);
            });
        } catch (Exception e) {
            LOG.error("failed to rebalance the topology");
        }
    }

    public void start() {
        executor.scheduleAtFixedRate(() -> {
            long current;
            if (checker.isOverLoad()) {
                current = System.currentTimeMillis();
                overloadTimes.add(current);
            } else {
                current = System.currentTimeMillis();
            }
            while (!overloadTimes.isEmpty() && overloadTimes.peek() + optimizeTimeInterval < current) {
                overloadTimes.remove();
            }
            if (overloadTimes.size() > 0.8 * sampleTimes && current > lastOptimizeTime + timeInterval) {
                optimize();
                overloadTimes.clear();
                lastOptimizeTime = current;
            }
        }, timeInterval, timeInterval, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        executor.shutdown();
    }
}
