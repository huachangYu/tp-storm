package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.daemon.LoadType;
import org.apache.storm.daemon.WorkerOptimizer;
import org.apache.storm.daemon.worker.SystemMonitor;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.strategy.ScheduleStrategy;
import org.apache.storm.executor.strategy.ScheduleStrategyUtils;
import org.apache.storm.executor.strategy.StrategyType;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ResizableBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledExecutorPool implements IScheduledExecutorPool {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExecutorPool.class);
    private static final AtomicInteger CONSUMER_INDEX = new AtomicInteger(0);
    private static final Random RAND = new Random();
    private static final long TOTAL_TIME_NS_PER_TASK_BATCH = 1000 * 1000; // 1ms
    private static final double HIGH_QUEUE_LOAD_THRESHOLD = 0.9;
    private static final double LOW_QUEUE_LOAD_THRESHOLD = 0.1;
    private static final double HIGH_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER = 0.7;
    private static final double LOW_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER = 0.1;
    private static final double HIGH_CPU_USAGE_THRESHOLD = 0.85;
    private static final double OTHER_THREADS_TOTAL_CPU_USAGE = 0.2;
    private static final double WORKER_HIGH_CPU_USAGE_THRESHOLD = 0.85;
    private static final double WORKER_LOW_CPU_USAGE_THRESHOLD = 0.15;

    private class BoltConsumer extends Thread {
        private volatile boolean running;

        BoltConsumer(String threadName) {
            super(threadName);
            this.running = true;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    List<BoltTask> tasks = getTask();
                    for (BoltTask task : tasks) {
                        task.run();
                        if (task.shouldRecordCost()) {
                            task.getMonitor().recordCost(task.getCostNs());
                        }
                    }
                    if (tasks.size() > 0) {
                        tasks.get(0).getMonitor().recordLastTime(System.nanoTime());
                    }
                } catch (InterruptedException e) {
                    LOG.warn("error occurred when getting or running task. ex:{}", e.getMessage());
                }
            }
            LOG.info("bolt consumer stopped");
        }

        public void close() {
            running = false;
        }
    }

    private final SystemMonitor systemMonitor;
    private final String topologyId;
    private final Map<String, Object> topologyConf;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptyThreadWait = lock.newCondition();
    private final Condition emptyQueueWait = lock.newCondition();
    private final int coreConsumers;
    private final int maxConsumers;
    private final int maxWorkers; // max number of worker in Storm cluster.
    private final List<BoltExecutor> bolts = new ArrayList<>();
    private final AtomicInteger blockedConsumerNum = new AtomicInteger();
    private final List<BoltConsumer> consumers;
    private final ConcurrentHashMap<String, TaskQueue> taskQueues;
    private final BooleanSupplier optimizeSample = ConfigUtils.sequenceSample(1000);
    private TaskQueueOptimizer taskQueueOptimizer;
    private WorkerOptimizer workerOptimizer;
    private Thread consumerOptimizer;
    private final int minQueueCapacity;
    private final boolean optimizePool;
    private final boolean optimizeWorker;
    private final ScheduleStrategy strategy;
    private Thread monitorThread;
    private final boolean shouldPrintMetrics;
    private final boolean updateStrategyStatus;

    public ScheduledExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf) {
        this.systemMonitor = systemMonitor;
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;

        final long coreConsumers = (Long) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_CORE_CONSUMERS,
                (long) SystemMonitor.CPU_CORE_NUM);
        final long maxConsumers = (Long) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_MAX_CONSUMERS, coreConsumers);
        final long maxWorkers = (Long) topologyConf.getOrDefault(Config.TOPOLOGY_MAX_WORKER_NUM, 1L);
        final long minQueueCapacity = (Long) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_MIN_QUEUE_CAPACITY, 1000L);
        if (maxConsumers < coreConsumers) {
            throw new IllegalArgumentException("maxConsumers should be not less than coreConsumers");
        }
        this.coreConsumers = (int) coreConsumers;
        this.maxConsumers = (int) maxConsumers;
        this.maxWorkers = (int) maxWorkers;
        this.minQueueCapacity = (int) minQueueCapacity;

        this.taskQueues = new ConcurrentHashMap<>(this.coreConsumers);
        this.consumers = new ArrayList<>(this.coreConsumers);
        this.optimizePool = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_BOLT_THREAD_POOL_OPTIMIZE, true);
        this.optimizeWorker = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_ENABLE_WORKERS_OPTIMIZE, true);
        if (this.optimizePool && topologyConf.containsKey(Config.BOLT_EXECUTOR_POOL_TOTAL_QUEUE_CAPACITY)) {
            long totalCapacity = (Long) topologyConf.get(Config.BOLT_EXECUTOR_POOL_TOTAL_QUEUE_CAPACITY);
            this.taskQueueOptimizer = new TaskQueueOptimizer(this.taskQueues, this.minQueueCapacity,
                    (int) totalCapacity, HIGH_QUEUE_LOAD_THRESHOLD, LOW_QUEUE_LOAD_THRESHOLD,
                    0.5, 0.5);
        }
        String strategyName = (String) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_STRATEGY,
                StrategyType.AD.name());
        this.strategy = ScheduleStrategyUtils.getStrategy(strategyName);
        while (consumers.size() < coreConsumers) {
            addConsumer();
        }
        if (optimizePool) {
            startOptimizeConsumer();
        }
        if (this.optimizeWorker) {
            startWorkerOptimizer();
        }

        this.updateStrategyStatus = strategyName.equals(StrategyType.AD.name());
        this.shouldPrintMetrics = (Boolean) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_PRINT_METRICS, false);
        if (shouldPrintMetrics || updateStrategyStatus) {
            startMonitorThread();
        }
    }

    private List<BoltTask> getTask() throws InterruptedException {
        lock.lock();
        try {
            while (bolts.isEmpty()) {
                emptyThreadWait.await();
            }
            List<BoltExecutor> notEmptyThreads = bolts.stream()
                    .filter(t -> taskQueues.get(t.getName()).getQueue().size() > 0)
                    .collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                blockedConsumerNum.getAndIncrement();
                emptyQueueWait.await();
                blockedConsumerNum.getAndDecrement();
                notEmptyThreads = bolts.stream()
                        .filter(t -> taskQueues.get(t.getName()).getQueue().size() > 0)
                        .collect(Collectors.toList());
            }

            int threadsNum = notEmptyThreads.size();
            int startExecutorThread = 0;
            if (threadsNum > 1) {
                startExecutorThread = RAND.nextInt(threadsNum);
            }
            BoltExecutor executor = notEmptyThreads.get(startExecutorThread);
            TaskQueue taskQueue = taskQueues.get(executor.getName());
            final long currentNs = System.nanoTime();
            for (int i = 0; i < threadsNum; i++) {
                if (i == startExecutorThread) {
                    continue;
                }
                BoltExecutor tmpExecutor = notEmptyThreads.get(i);
                TaskQueue tmpTaskQueue = taskQueues.get(tmpExecutor.getName());
                if (strategy.compare(taskQueue, tmpTaskQueue, currentNs) < 0) {
                    taskQueue = tmpTaskQueue;
                    executor = tmpExecutor;
                }
            }
            ResizableBlockingQueue<BoltTask> queue = taskQueue.getQueue();
            List<BoltTask> tasks = new ArrayList<>();
            double avgTimeNs = executor.getMonitor().getAvgTimeNs();
            int batchSize = avgTimeNs <= 10 ? 1 : (int) Math.max(1, Math.ceil(TOTAL_TIME_NS_PER_TASK_BATCH / avgTimeNs));
            while (!queue.isEmpty() && tasks.size() < batchSize) {
                tasks.add(queue.remove());
            }
            return tasks;
        } finally {
            lock.unlock();
        }
    }

    public void addThread(BoltExecutor thread) {
        lock.lock();
        try {
            String threadName = thread.getName();
            if (taskQueues.containsKey(threadName)) {
                return;
            }
            bolts.add(thread);
            taskQueues.put(threadName, new TaskQueue(minQueueCapacity, thread.getReceiveQueue(), thread.getMonitor()));
            if (taskQueues.size() == 1) {
                emptyThreadWait.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void removeThread(String threadName) {
        lock.lock();
        try {
            taskQueues.remove(threadName);
            bolts.removeIf(t -> t.getName().equals(threadName));
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        if (this.workerOptimizer != null) {
            this.workerOptimizer.shutdown();
        }
        for (BoltConsumer consumer : consumers) {
            consumer.close();
        }
    }

    public void submit(String threadName, BoltTask task) {
        if (optimizePool && optimizeSample.getAsBoolean()) {
            lock.lock();
            taskQueueOptimizer.optimize();
            lock.unlock();
        }
        try {
            taskQueues.get(threadName).getQueue().put(task);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (blockedConsumerNum.get() > 0) {
            signalQueueNotEmpty();
        }
    }

    private void addConsumer() {
        lock.lock();
        try {
            if (consumers.size() < maxConsumers) {
                BoltConsumer consumer = new BoltConsumer("bolt-consumer-" + CONSUMER_INDEX.getAndIncrement());
                consumers.add(consumer);
                consumer.setPriority(Thread.MAX_PRIORITY);
                consumer.setDaemon(true);
                consumer.start();
                LOG.info("added a new consumer[{}]", consumer.getName());
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeConsumer() {
        lock.lock();
        try {
            if (consumers.size() > coreConsumers) {
                BoltConsumer consumer = consumers.remove(consumers.size() - 1);
                consumer.close();
                LOG.info("removed the consumer[{}]", consumer.getName());
            }
        } finally {
            lock.unlock();
        }
    }

    private void startOptimizeConsumer() {
        this.consumerOptimizer = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                lock.lock();
                optimizeConsumer();
                lock.unlock();
            }
        });
        this.consumerOptimizer.setDaemon(true);
        this.consumerOptimizer.start();
    }

    // thread-unsafe
    private void optimizeConsumer() {
        if (taskQueues.size() == 0) {
            return;
        }
        double[] queueLoads = taskQueues.values().stream().mapToDouble(t -> t.getQueue().getLoad()).toArray();
        boolean isOverLoad = Arrays.stream(queueLoads).anyMatch(t -> t > HIGH_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER);
        boolean isLowLoad = Arrays.stream(queueLoads).allMatch(t -> t < LOW_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER);
        if ((!isOverLoad && !isLowLoad)) {
            return;
        }
        long flowSurgeCount = taskQueues.values().stream().filter(t -> t.getMonitor().isFlowSurge()).count();
        if (consumers.size() < maxConsumers && 2 * flowSurgeCount > taskQueues.size()) {
            addConsumer();
            LOG.info("optimize consumer. added a new consumer because the flow surged. "
                    + "consumerNum:{}", consumers.size());
            for (TaskQueue taskQueue : taskQueues.values()) {
                taskQueue.getMonitor().clearPeriodWindow();
            }
            return;
        }
        double cpuUsage = systemMonitor.getAverageCpuUsage(10);
        if (consumers.size() < maxConsumers && isOverLoad
                && cpuUsage > 0 && cpuUsage < HIGH_CPU_USAGE_THRESHOLD) {
            addConsumer();
            LOG.info("optimize consumer. added a new consumer because executor pool is overload. "
                    + "cpu usage:{}, consumerNum:{}", String.format("%.4f", cpuUsage), consumers.size());
            for (TaskQueue taskQueue : taskQueues.values()) {
                taskQueue.getMonitor().clearPeriodWindow();
            }
        } else if (consumers.size() > coreConsumers && isLowLoad
                && cpuUsage / (consumers.size() - 1)
                    < (1 - OTHER_THREADS_TOTAL_CPU_USAGE) / SystemMonitor.CPU_CORE_NUM) {
            removeConsumer();
            LOG.info("optimize consumer. removed a consumer because executor pool is in low load. "
                    + "cpu usage:{}, consumerNum: {}", String.format("%.4f", cpuUsage), consumers.size());
        }
    }

    private void startWorkerOptimizer() {
        String[] parts = topologyId.split("-");
        if (parts.length < 3) {
            LOG.warn("wrong topology id: {}", topologyId);
            return;
        }
        String topologyName = StringUtils.join(Arrays.copyOfRange(parts, 0, parts.length - 2), "-");
        this.workerOptimizer = new WorkerOptimizer(topologyName, topologyConf, maxWorkers, () -> {
            lock.lock();
            try {
                double[] queueLoads = taskQueues.values().stream().mapToDouble(t -> t.getQueue().getLoad()).toArray();
                boolean isQueueHighLoad = Arrays.stream(queueLoads).anyMatch(t -> t > HIGH_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER);
                boolean isQueueLowLoad = Arrays.stream(queueLoads).allMatch(t -> t < LOW_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER);
                double cpuUsage = systemMonitor.getAverageCpuUsage(10);
                if (isQueueHighLoad
                        && cpuUsage / consumers.size() > WORKER_HIGH_CPU_USAGE_THRESHOLD / SystemMonitor.CPU_CORE_NUM) {
                    return LoadType.High;
                } else if (isQueueLowLoad
                        && cpuUsage / consumers.size() < WORKER_LOW_CPU_USAGE_THRESHOLD / SystemMonitor.CPU_CORE_NUM) {
                    return LoadType.Low;
                } else {
                    return LoadType.Mid;
                }
            } finally {
                lock.unlock();
            }
        });
        this.workerOptimizer.start();
    }

    private void printMetrics() {
        // print queue info
        LOG.info("queue info: {}.", Arrays.toString(taskQueues.entrySet().stream()
                .map(t -> {
                    ResizableBlockingQueue<BoltTask> queue = t.getValue().getQueue();
                    return String.format("\"%s\":(%d/%d)",
                            t.getKey(), queue.size(), queue.getCapacity());
                }).toArray()));
    }

    private void startMonitorThread() {
        this.monitorThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                lock.lock();
                try {
                    if (shouldPrintMetrics) {
                        printMetrics();
                    }
                    if (updateStrategyStatus) {
                        boolean isQueueHighLoad = taskQueues.values().stream().anyMatch(
                            t -> t.getQueue().getLoad() > HIGH_QUEUE_LOAD_THRESHOLD_FOR_CONSUMER);
                        if (strategy.getStatus() == 0 && isQueueHighLoad) {
                            LOG.info("update status of strategy from 0 to 1");
                            strategy.setStatus(1);
                        } else if (strategy.getStatus() == 1 && !isQueueHighLoad) {
                            LOG.info("update status of strategy from 1 to 0");
                            strategy.setStatus(0);
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        });
        this.monitorThread.setDaemon(true);
        this.monitorThread.start();
    }

    private void signalQueueNotEmpty() {
        lock.lock();
        try {
            emptyQueueWait.signal();
        } finally {
            lock.unlock();
        }
    }
}
