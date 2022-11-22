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
import org.apache.storm.daemon.OverLoadChecker;
import org.apache.storm.daemon.WorkerOptimizer;
import org.apache.storm.daemon.worker.SystemMonitor;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ResizableBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledExecutorPool implements IScheduledExecutorPool {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExecutorPool.class);
    private static final Random RAND = new Random();
    private static long totalTimeNsPerBatch = 1000 * 1000; // 1ms

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
    private final AtomicInteger consumerIndex = new AtomicInteger(0);
    private final ConcurrentHashMap<String, TaskQueue> taskQueues;
    private final BooleanSupplier optimizeSample = ConfigUtils.sequenceSample(1000);
    private TaskQueueOptimizer taskQueueOptimizer;
    private WorkerOptimizer workerOptimizer;
    private Thread consumerOptimizer;
    private final int minQueueCapacity;
    private final boolean optimizePool;
    private final boolean optimizeWorker;
    private final IScheduleStrategy strategy;
    private Thread metricsThread;

    public ScheduledExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf) {
        this(systemMonitor, topologyId, topologyConf, SystemMonitor.CPU_CORE_NUM,
                2 * SystemMonitor.CPU_CORE_NUM, 1);
    }

    public ScheduledExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf,
                                 int coreConsumers, int maxConsumers, int maxWorkers) {
        this.systemMonitor = systemMonitor;
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.minQueueCapacity = ((Long) topologyConf.getOrDefault(
                Config.BOLT_EXECUTOR_POOL_MIN_QUEUE_CAPACITY, 1000L)).intValue();
        this.coreConsumers = coreConsumers;
        this.maxConsumers = maxConsumers;
        this.maxWorkers = maxWorkers;
        this.taskQueues = new ConcurrentHashMap<>(coreConsumers);
        this.consumers = new ArrayList<>(coreConsumers);
        this.optimizePool = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_BOLT_THREAD_POOL_OPTIMIZE, true);
        this.optimizeWorker = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_ENABLE_WORKERS_OPTIMIZE, true);
        if (this.optimizePool && topologyConf.containsKey(Config.BOLT_EXECUTOR_POOL_TOTAL_QUEUE_CAPACITY)) {
            long totalCapacity = (Long) topologyConf.get(Config.BOLT_EXECUTOR_POOL_TOTAL_QUEUE_CAPACITY);
            this.taskQueueOptimizer = new TaskQueueOptimizer(taskQueues, minQueueCapacity, (int) totalCapacity,
                    0.9, 0.1, 0.5, 0.5);
        }
        String strategyName = (String) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_STRATEGY,
                ScheduledStrategy.Strategy.Fair.name());
        this.strategy = ScheduledStrategy.getStrategy(strategyName);
        while (consumers.size() < coreConsumers) {
            addConsumer();
        }
        if (optimizePool) {
            startOptimizeConsumer();
        }
        if (this.optimizeWorker) {
            startWorkerOptimizer();
        }

        boolean printMetrics = (Boolean) topologyConf.getOrDefault(Config.BOLT_EXECUTOR_POOL_PRINT_METRICS, false);
        if (printMetrics) {
            this.metricsThread = new Thread(() -> {
                while (true) {
                    printMetrics();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            this.metricsThread.setDaemon(true);
            this.metricsThread.start();
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
            int batchSize = avgTimeNs <= 10 ? 1 : (int) Math.max(1, Math.ceil(totalTimeNsPerBatch / avgTimeNs));
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
            taskQueues.put(threadName, new TaskQueue(minQueueCapacity, thread.getMonitor()));
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
        if (this.consumerOptimizer != null) {
            this.consumerOptimizer.interrupt();
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

    private void signalQueueNotEmpty() {
        lock.lock();
        try {
            emptyQueueWait.signal();
        } finally {
            lock.unlock();
        }
    }

    private void addConsumer() {
        lock.lock();
        try {
            if (consumers.size() < maxConsumers) {
                BoltConsumer consumer = new BoltConsumer("bolt-consumer-" + consumerIndex.getAndIncrement());
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
        consumerOptimizer = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                lock.lock();
                optimizeConsumer();
                lock.unlock();
            }
        });
        consumerOptimizer.setDaemon(true);
        consumerOptimizer.start();
    }

    // thread-unsafe
    private void optimizeConsumer() {
        if (consumers.size() >= maxConsumers || taskQueues.size() == 0) {
            return;
        }
        int queueNum = taskQueues.size();
        int[] capacities = taskQueues.values().stream().mapToInt(t -> t.getQueue().getCapacity()).toArray();
        int[] sizes = taskQueues.values().stream().mapToInt(t -> t.getQueue().size()).toArray();
        double[] workloads = new double[queueNum];
        for (int  i = 0; i < queueNum; i++) {
            workloads[i] = (double) capacities[i] / (double) Math.max(sizes[i], 1);
        }
        int sumCapacity = Arrays.stream(capacities).sum();
        boolean isOverLoad = Arrays.stream(workloads).anyMatch(t -> t > 0.9);
        boolean isLowLoad = Arrays.stream(workloads).allMatch(t -> t < 0.1);
        boolean hasRemainCapacity = taskQueueOptimizer.getRemainCapacity() > 0.5 * sumCapacity;
        if ((!isOverLoad && !isLowLoad) || hasRemainCapacity) {
            return;
        }
        double cpuUsage = systemMonitor.getCpuUsage();
        if (isOverLoad && cpuUsage > 0 && cpuUsage < 0.8) {
            addConsumer();
            LOG.info("optimize consumer. Added a new consumer");
        } else if (isLowLoad && consumers.size() > coreConsumers) {
            removeConsumer();
            LOG.info("optimize consumer. Removed a consumer");
        }
    }

    private void startWorkerOptimizer() {
        String[] parts = topologyId.split("-");
        if (parts.length < 3) {
            LOG.warn("wrong topology id: {}", topologyId);
            return;
        }
        String topologyName = StringUtils.join(Arrays.copyOfRange(parts, 0, parts.length - 2), "-");
        this.workerOptimizer = new WorkerOptimizer(topologyName, topologyConf, maxWorkers, new OverLoadChecker() {
            @Override
            public LoadType getLoadType() {
                lock.lock();
                try {
                    int[] capacities = taskQueues.values().stream().mapToInt(t -> t.getQueue().getCapacity()).toArray();
                    int[] sizes = taskQueues.values().stream().mapToInt(t -> t.getQueue().size()).toArray();
                    double[] workloads = new double[taskQueues.size()];
                    for (int  i = 0; i < taskQueues.size(); i++) {
                        workloads[i] = (double) capacities[i] / (double) Math.max(sizes[i], 1);
                    }
                    boolean isQueueHighLoad = Arrays.stream(workloads).anyMatch(t -> t > 0.9);
                    boolean isQueueLowLoad = Arrays.stream(workloads).allMatch(t -> t < 0.1);
                    double cpuUsage = systemMonitor.getCpuUsage();
                    if (isQueueHighLoad && cpuUsage > 0.7) {
                        return LoadType.High;
                    } else if (isQueueLowLoad && cpuUsage < 0.1) {
                        return LoadType.Low;
                    } else {
                        return LoadType.Mid;
                    }
                } finally {
                    lock.unlock();
                }
            }
        });
        this.workerOptimizer.start();
    }

    private void printMetrics() {
        lock.lock();
        try {
            // print queue info
            List<String> queueInfos = taskQueues.entrySet().stream()
                    .map(t -> {
                        ResizableBlockingQueue<BoltTask> queue = t.getValue().getQueue();
                        return String.format("\"%s\":(%d/%d)",
                                t.getKey(), queue.size(), queue.getCapacity());
                    }).collect(Collectors.toList());
            LOG.info("queue info: {}.", Arrays.toString(queueInfos.toArray()));
        } finally {
            lock.unlock();
        }
    }
}
