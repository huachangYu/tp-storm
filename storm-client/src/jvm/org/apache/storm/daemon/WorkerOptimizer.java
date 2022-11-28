package org.apache.storm.daemon;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerOptimizer implements Shutdownable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerOptimizer.class);
    private final String topologyName;
    private final Map<String, Object> topologyConf;
    private final int maxWorkers;
    private final long timeInterval = 10;
    private final long optimizeTimeInterval = 120 * 1000;
    private final double highLoadThreshold = 0.75;
    private final double lowLoadThreshold = 0.75;
    private long lastOptimizeTime = System.currentTimeMillis();
    private final Queue<Long> highLoadRecords = new LinkedList<>();
    private final Queue<Long> midLoadRecords = new LinkedList<>();
    private final Queue<Long> lowLoadRecords = new LinkedList<>();
    private final OverLoadChecker checker;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public WorkerOptimizer(String topologyName, Map<String, Object> topologyConf, int maxWorkers, OverLoadChecker checker) {
        this.topologyName = topologyName;
        this.topologyConf = topologyConf;
        this.maxWorkers = maxWorkers;
        this.checker = checker;
    }

    private void optimize(int increase) {
        long workerNum = (Long) topologyConf.getOrDefault(Config.TOPOLOGY_WORKERS, 1);
        final int newWorkerNum = (int) workerNum + increase;
        if (newWorkerNum > maxWorkers || newWorkerNum < 1) {
            return;
        }
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
            LoadType loadType = checker.getLoadType();
            current = System.currentTimeMillis();
            if (loadType == LoadType.High) {
                highLoadRecords.add(current);
            } else if (loadType == LoadType.Mid) {
                midLoadRecords.add(current);
            } else if (loadType == LoadType.Low) {
                lowLoadRecords.add(current);
            }
            while (!highLoadRecords.isEmpty() && highLoadRecords.peek() + optimizeTimeInterval < current) {
                highLoadRecords.remove();
            }
            while (!midLoadRecords.isEmpty() && midLoadRecords.peek() + optimizeTimeInterval < current) {
                midLoadRecords.remove();
            }
            while (!lowLoadRecords.isEmpty() && lowLoadRecords.peek() + optimizeTimeInterval < current) {
                lowLoadRecords.remove();
            }
            if (current > lastOptimizeTime + optimizeTimeInterval) {
                int totalRecords = highLoadRecords.size() + midLoadRecords.size() + lowLoadRecords.size();
                if (highLoadRecords.size() > highLoadThreshold * totalRecords) {
                    optimize(1);
                    highLoadRecords.clear();
                    midLoadRecords.clear();
                    lastOptimizeTime = current;
                } else if (lowLoadRecords.size() > lowLoadThreshold * totalRecords) {
                    optimize(-1);
                    highLoadRecords.clear();
                    midLoadRecords.clear();
                    lastOptimizeTime = current;
                }
            }
        }, timeInterval, timeInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        LOG.info("worker optimizer shutdown!");
    }
}
