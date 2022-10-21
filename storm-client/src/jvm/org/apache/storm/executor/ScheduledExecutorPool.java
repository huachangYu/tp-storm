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
import org.apache.storm.daemon.OverLoadChecker;
import org.apache.storm.daemon.WorkerOptimizer;
import org.apache.storm.daemon.worker.SystemMonitor;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ResizableLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledExecutorPool implements IScheduledExecutorPool {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExecutorPool.class);
    private static final Random RAND = new Random();

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
                    List<BoltTask> tasks = getTask(maxTasks);
                    for (BoltTask task : tasks) {
                        task.run();
                        if (task.shouldRecordCost()) {
                            task.getMonitor().recordCost(task.getCostNs());
                        }
                        if (task.shouldRecordTaskInfo()) {
                            task.getMonitor().recordTaskInfo(task, taskQueues.get(task.getThreadName()).size());
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
    private final ConcurrentHashMap<String, ResizableLinkedBlockingQueue<BoltTask>> taskQueues;
    private final int maxTasks;
    private final BooleanSupplier optimizeSample = ConfigUtils.sequenceSample(2000);
    private WorkerOptimizer workerOptimizer;
    private long lastTimeNsUpdateConsumer = System.nanoTime();
    private long timeIntervalNsUpdateConsumer = 5000L * 1000 * 1000; //5s
    private final int minQueueCapacity;
    private final boolean optimizePool;
    private final boolean optimizeWorker;
    private final ScheduledStrategy.Strategy strategy;

    public ScheduledExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf) {
        this(systemMonitor, topologyId, topologyConf, SystemMonitor.CPU_CORE_NUM,
                2 * SystemMonitor.CPU_CORE_NUM, 1, 1);
    }

    public ScheduledExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf,
                                 int coreConsumers, int maxConsumers, int maxWorkers, int maxTasks) {
        this.systemMonitor = systemMonitor;
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.minQueueCapacity = ((Long) topologyConf.getOrDefault(
                Config.TOPOLOGY_BOLT_THREAD_POOL_MIN_QUEUE_CAPACITY, 1000L)).intValue();
        this.coreConsumers = coreConsumers;
        this.maxConsumers = maxConsumers;
        this.maxWorkers = maxWorkers;
        this.taskQueues = new ConcurrentHashMap<>(coreConsumers);
        this.consumers = new ArrayList<>(coreConsumers);
        this.maxTasks = maxTasks;
        this.optimizePool = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_BOLT_THREAD_POOL_OPTIMIZE, true);
        this.optimizeWorker = (Boolean) topologyConf.getOrDefault(Config.TOPOLOGY_ENABLE_WORKERS_OPTIMIZE, true);
        if (this.optimizePool && topologyConf.containsKey(Config.TOPOLOGY_BOLT_THREAD_POOL_TOTAL_QUEUE_CAPACITY)) {
            long totalCapacity = (Long) topologyConf.get(Config.TOPOLOGY_BOLT_THREAD_POOL_TOTAL_QUEUE_CAPACITY);
            BoltExecutorOptimizerUtil.setMaxTotalCapacity((int) totalCapacity);
        }
        String strategyName = (String) topologyConf.getOrDefault(Config.TOPOLOGY_BOLT_THREAD_POOL_STRATEGY,
                ScheduledStrategy.Strategy.Fair.name());
        this.strategy = ScheduledStrategy.Strategy.valueOf(strategyName);
        while (consumers.size() < coreConsumers) {
            addConsumer();
        }
        if (this.optimizeWorker) {
            startWorkerOptimizer();
        }
    }

    private List<BoltTask> getTask(int maxTaskSize) throws InterruptedException {
        lock.lock();
        try {
            while (bolts.isEmpty()) {
                emptyThreadWait.await();
            }
            List<BoltExecutor> notEmptyThreads = bolts.stream()
                    .filter(t -> taskQueues.get(t.getName()).size() > 0)
                    .collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                blockedConsumerNum.getAndIncrement();
                emptyQueueWait.await();
                blockedConsumerNum.getAndDecrement();
                notEmptyThreads = bolts.stream()
                        .filter(t -> taskQueues.get(t.getName()).size() > 0)
                        .collect(Collectors.toList());
            }

            int threadsNum = notEmptyThreads.size();
            int startExecutorThread = 0;
            if (threadsNum > 1) {
                startExecutorThread = RAND.nextInt(threadsNum);
            }
            BoltExecutor executor = notEmptyThreads.get(startExecutorThread);
            ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(executor.getName());
            final long currentNs = System.nanoTime();
            for (int i = 0; i < threadsNum; i++) {
                if (i == startExecutorThread) {
                    continue;
                }
                BoltExecutor tmpExecutor = notEmptyThreads.get(i);
                ResizableLinkedBlockingQueue<BoltTask> tmpQueue = taskQueues.get(tmpExecutor.getName());
                if (ScheduledStrategy.compare(queue, tmpQueue, executor.getMonitor(), tmpExecutor.getMonitor(),
                        currentNs, strategy) > 0) {
                    queue = tmpQueue;
                    executor = tmpExecutor;
                }
            }
            List<BoltTask> tasks = new ArrayList<>();
            while (!queue.isEmpty() && tasks.size() < maxTaskSize) {
                tasks.add(queue.remove());
            }
            if (optimizePool && optimizeSample.getAsBoolean()) {
                optimize(currentNs);
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
            taskQueues.put(threadName, new ResizableLinkedBlockingQueue<>(minQueueCapacity));
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
        try {
            taskQueues.get(threadName).put(task);
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

    private void optimize(long currentNs) {
        lock.lock();
        try {
            optimizeConsumer(currentNs);
            optimizeQueueSize(currentNs);
        } finally {
            lock.unlock();
        }
    }

    // thread-unsafe
    private void optimizeConsumer(long currentNs) {
        if (consumers.size() >= maxConsumers || currentNs < lastTimeNsUpdateConsumer + timeIntervalNsUpdateConsumer) {
            return;
        }
        int sumCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getCapacity).sum();
        int sumSize = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::size).sum();
        double cpuUsage = systemMonitor.getAvgCpuUsage();
        if (sumSize >= 0.8 * sumCapacity && cpuUsage > 0 && cpuUsage < 0.7) {
            addConsumer();
            lastTimeNsUpdateConsumer = currentNs;
            LOG.info("optimize consumer. Added a new consumer");
        } else if (sumSize <= 0.1 * sumCapacity && consumers.size() > coreConsumers) {
            removeConsumer();
            lastTimeNsUpdateConsumer = currentNs;
            LOG.info("optimize consumer. Removed a consumer");
        }
    }

    // thread-unsafe
    private void optimizeQueueSize(long currentNs) {
        Map<String, Integer> increase = BoltExecutorOptimizerUtil.getIncreaseBaseOnArima(taskQueues, bolts,
                minQueueCapacity, currentNs);
        if (increase.size() == 0) {
            return;
        }
        for (String queueName : increase.keySet()) {
            ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
            queue.resizeQueue(queue.getCapacity() + increase.get(queueName));
        }
        int[] queueCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getCapacity).toArray();
        int[] queueSizes = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::size).toArray();
        LOG.info("optimize queue size. diff={}. new capacity={}. size={}.",
                JSONObject.toJSONString(increase), Arrays.toString(queueCapacity),
                Arrays.toString(queueSizes));
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
            public boolean isOverLoad() {
                lock.lock();
                try {
                    int sumCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getCapacity).sum();
                    int sumSize = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::size).sum();
                    //TODO add cpu usage?
                    return sumSize >= 0.9 * sumCapacity;
                } finally {
                    lock.unlock();
                }
            }
        });
        this.workerOptimizer.start();
    }
}
