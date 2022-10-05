package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.storm.daemon.OverLoadChecker;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.WorkerOptimizer;
import org.apache.storm.daemon.worker.SystemMonitor;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ResizableLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltExecutorPool implements Shutdownable {
    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutorPool.class);
    private static final double EPS = 1e-5;
    private static final int DEFAULT_MIN_QUEUE_CAPACITY = 10000;
    private static final int DEFAULT_MAX_QUEUE_CAPACITY = 100000;

    private class BoltConsumer extends Thread {
        private boolean running;

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
                            task.getMonitor().recordCost(task.getCost());
                        }
                        if (task.shouldRecordTaskInfo()) {
                            task.getMonitor().recordTaskInfo(task, taskQueues.get(task.getThreadName()).size());
                        }
                    }
                    if (tasks.size() > 0) {
                        tasks.get(0).getMonitor().recordLastTime(System.currentTimeMillis());
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
    private final boolean autoOptimize;
    private final BooleanSupplier optimizeSample = ConfigUtils.sequenceSample(2000);
    private WorkerOptimizer workerOptimizer;
    private long lastTimeUpdateConsumer = System.currentTimeMillis();
    private long timeIntervalUpdateConsumer = 5000;

    public BoltExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf) {
        this(systemMonitor, topologyId, topologyConf, SystemMonitor.CPU_CORE_NUM,
                2 * SystemMonitor.CPU_CORE_NUM, 1, 1, true);
    }

    public BoltExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf,
                            int coreConsumers, int maxConsumers, int maxWorkers, int maxTasks) {
        this(systemMonitor, topologyId, topologyConf, coreConsumers, maxConsumers,
                maxWorkers, maxTasks, true);
    }

    public BoltExecutorPool(SystemMonitor systemMonitor, String topologyId, Map<String, Object> topologyConf,
                            int coreConsumers, int maxConsumers, int maxWorkers, int maxTasks,
                            boolean autoOptimize) {
        this.systemMonitor = systemMonitor;
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.coreConsumers = coreConsumers;
        this.maxConsumers = maxConsumers;
        this.maxWorkers = maxWorkers;
        this.taskQueues = new ConcurrentHashMap<>(coreConsumers);
        this.consumers = new ArrayList<>(coreConsumers);
        this.maxTasks = maxTasks;
        this.autoOptimize = autoOptimize;
        while (consumers.size() < coreConsumers) {
            addConsumer();
        }
        startWorkerOptimizer();
    }

    public List<BoltTask> getTask(int maxTaskSize) throws InterruptedException {
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

            final long current = System.currentTimeMillis();
            BoltExecutor firstThread = notEmptyThreads.get(0);
            int minTaskQueueSize = taskQueues.get(firstThread.getName()).size();
            int maxTaskQueueSize = minTaskQueueSize;
            double minAvgTime = firstThread.getMonitor().getAvgTime();
            double maxAvgTime = minAvgTime;
            long minWaitingTime = firstThread.getMonitor().getWaitingTime(current);
            long maxWaitingTime = minWaitingTime;
            for (int i = 1; i < notEmptyThreads.size(); i++) {
                int queueSize = taskQueues.get(notEmptyThreads.get(i).getName()).size();
                minTaskQueueSize = Math.min(minTaskQueueSize, queueSize);
                maxTaskQueueSize = Math.max(maxTaskQueueSize, queueSize);
                double avgTime = notEmptyThreads.get(i).getMonitor().getAvgTime();
                minAvgTime = Math.min(minAvgTime, avgTime);
                maxAvgTime = Math.max(maxAvgTime, avgTime);
                long waitingTime = notEmptyThreads.get(i).getMonitor().getWaitingTime(current);
                minWaitingTime = Math.min(minWaitingTime, waitingTime);
                maxWaitingTime = Math.max(maxWaitingTime, waitingTime);
            }
            double maxWeight = 0.0;
            for (BoltExecutor boltExecutor : notEmptyThreads) {
                ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(boltExecutor.getName());
                double weight = boltExecutor.getMonitor().updateWeight(
                        current, queue.size(), queue.getMaximumQueueSize(),
                        minTaskQueueSize, maxTaskQueueSize,
                        minAvgTime, maxAvgTime,
                        minWaitingTime, maxWaitingTime);
                maxWeight = Math.max(maxWeight, weight);
            }
            List<BoltTask> tasks = new ArrayList<>();
            List<BoltExecutor> maxWeightBoltThreads = new ArrayList<>();
            for (BoltExecutor boltExecutor : notEmptyThreads) {
                if (Math.abs(boltExecutor.getWeight() - maxWeight) < EPS) {
                    maxWeightBoltThreads.add(boltExecutor);
                }
            }
            Queue<BoltTask> queue = taskQueues.get(maxWeightBoltThreads.get(
                    ThreadLocalRandom.current().nextInt(maxWeightBoltThreads.size())).getName());
            while (!queue.isEmpty() && tasks.size() < maxTaskSize) {
                tasks.add(queue.remove());
            }
            if (autoOptimize && optimizeSample.getAsBoolean()) {
                optimize(current);
            }
            return tasks;
        } finally {
            lock.unlock();
        }
    }

    public void addThreads(List<BoltExecutor> threads) {
        for (BoltExecutor thread : threads) {
            addThread(thread);
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
            taskQueues.put(threadName, new ResizableLinkedBlockingQueue<>(DEFAULT_MIN_QUEUE_CAPACITY));
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
        this.workerOptimizer.shutdown();
        for (BoltConsumer consumer : consumers) {
            consumer.close();
        }
    }

    public void submit(String threadName, BoltTask task) throws InterruptedException {
        taskQueues.get(threadName).put(task);
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

    private void optimize(long current) {
        lock.lock();
        try {
            optimizeConsumer(current);
            optimizeQueueSize(current);
        } finally {
            lock.unlock();
        }
    }

    // thread-unsafe
    private void optimizeConsumer(long current) {
        if (consumers.size() >= maxConsumers || current < lastTimeUpdateConsumer + timeIntervalUpdateConsumer) {
            return;
        }
        int sumCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getMaximumQueueSize).sum();
        int sumSize = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::size).sum();
        double cpuUsage = systemMonitor.getAvgCpuUsage();
        if (sumSize >= 0.8 * sumCapacity && cpuUsage > 0 && cpuUsage < 0.7) {
            addConsumer();
            lastTimeUpdateConsumer = current;
            LOG.info("optimize consumer. Added a new consumer");
        } else if (sumSize <= 0.1 * sumCapacity && consumers.size() > coreConsumers) {
            removeConsumer();
            lastTimeUpdateConsumer = current;
            LOG.info("optimize consumer. Removed a consumer");
        }
    }

    // thread-unsafe
    private void optimizeQueueSize(long current) {
        Map<String, Integer> increase = BoltExecutorOptimizerUtil.getIncreaseBaseOnArima(taskQueues, bolts,
                DEFAULT_MIN_QUEUE_CAPACITY, DEFAULT_MAX_QUEUE_CAPACITY, current);
        if (increase.size() == 0) {
            return;
        }
        for (String queueName : increase.keySet()) {
            ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
            queue.resizeQueue(queue.getMaximumQueueSize() + increase.get(queueName));
        }
        int[] queueCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getMaximumQueueSize).toArray();
        int[] queueSizes = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::size).toArray();
        LOG.info("optimize queue size. diff={}. new capacity={}. size={}.",
                JSONObject.toJSONString(increase), Arrays.toString(queueCapacity),
                Arrays.toString(queueSizes));
    }

    private void startWorkerOptimizer() {
        String[] parts = topologyId.split("-");
        if (parts.length < 3) {
            return;
        }
        String topologyName = StringUtils.join(Arrays.copyOfRange(parts, 0, parts.length - 2), "-");
        this.workerOptimizer = new WorkerOptimizer(topologyName, topologyConf, maxWorkers, new OverLoadChecker() {
            @Override
            public boolean isOverLoad() {
                lock.lock();
                try {
                    int sumCapacity = taskQueues.values().stream().mapToInt(ResizableLinkedBlockingQueue::getMaximumQueueSize).sum();
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
