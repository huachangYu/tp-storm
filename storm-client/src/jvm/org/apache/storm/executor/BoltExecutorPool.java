package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.worker.SystemMonitor;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ResizableLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltExecutorPool implements Shutdownable {
    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutorPool.class);
    private static final int CPU_NUM = Runtime.getRuntime().availableProcessors();
    private static final double EPS = 1e-5;
    private static final int DEFAULT_MAX_QUEUE_CAPACITY = 100000;

    private class BoltConsumer extends Thread {
        private boolean running;
        private final BooleanSupplier sampler = ConfigUtils.evenSampler(1000);

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
                    if (autoOptimize && sampler.getAsBoolean()) {
                        optimize();
                    }
                } catch (InterruptedException e) {
                    LOG.warn("error occurred when getting task. ex:{}", e.getMessage());
                }
            }
            LOG.info("bolt consumer stopped");
        }

        public void close() {
            running = false;
        }
    }

    private final SystemMonitor systemMonitor;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptyThreadWait = lock.newCondition();
    private final Condition emptyQueueWait = lock.newCondition();
    private final int coreConsumers;
    private final int maxConsumers;
    private final List<BoltExecutor> threads = new ArrayList<>();
    private final AtomicInteger blockedConsumerNum = new AtomicInteger();
    private final List<BoltConsumer> consumers;
    private final AtomicInteger consumerIndex = new AtomicInteger(0);
    private final ConcurrentHashMap<String, ResizableLinkedBlockingQueue<BoltTask>> taskQueues;
    private final int maxTasks;

    private final boolean autoOptimize;

    public BoltExecutorPool(SystemMonitor systemMonitor) {
        this(systemMonitor, CPU_NUM, 2 * CPU_NUM, 1, true);
    }

    public BoltExecutorPool(SystemMonitor systemMonitor, int coreConsumers, int maxConsumers, int maxTasks) {
        this(systemMonitor, coreConsumers, maxConsumers, maxTasks, true);
    }

    public BoltExecutorPool(SystemMonitor systemMonitor, int coreConsumers, int maxConsumers, int maxTasks, boolean autoOptimize) {
        this.systemMonitor = systemMonitor;
        this.coreConsumers = coreConsumers;
        this.maxConsumers = maxConsumers;
        this.taskQueues = new ConcurrentHashMap<>(coreConsumers);
        this.consumers = new ArrayList<>(coreConsumers);
        this.maxTasks = maxTasks;
        this.autoOptimize = autoOptimize;
        while (consumers.size() < coreConsumers) {
            addConsumer();
        }
    }

    public List<BoltTask> getTask(int maxTaskSize) throws InterruptedException {
        lock.lock();
        try {
            while (threads.isEmpty()) {
                emptyThreadWait.await();
            }
            List<BoltExecutor> notEmptyThreads = threads.stream()
                    .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                blockedConsumerNum.getAndIncrement();
                emptyQueueWait.await();
                blockedConsumerNum.getAndDecrement();
                notEmptyThreads = threads.stream()
                        .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
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
                double weight = boltExecutor.getMonitor().calculateWeight(current, taskQueues.get(boltExecutor.getName()).size(),
                        minTaskQueueSize, maxTaskQueueSize, minAvgTime, maxAvgTime, minWaitingTime, maxWaitingTime);
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
            threads.add(thread);
            taskQueues.put(threadName, new ResizableLinkedBlockingQueue<>(DEFAULT_MAX_QUEUE_CAPACITY));
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
            threads.removeIf(t -> t.getName().equals(threadName));
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
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

    private void optimize() {
        lock.lock();
        try {
            optimizeConsumer();
            optimizeQueueSize();
            optimizeWorkers();
        } finally {
            lock.unlock();
        }
    }

    // thread-unsafe
    private void optimizeConsumer() {
        if (consumers.size() >= maxConsumers) {
            return;
        }
        int sumCapacity = 0;
        int sumSize = 0;
        for (ResizableLinkedBlockingQueue<BoltTask> queue : taskQueues.values()) {
            sumCapacity += queue.getMaximumQueueSize();
            sumSize += queue.size();
        }
        double cpuUsage = systemMonitor.getAvgCpuUsage();
        System.out.println("cpu usage in 1s = " + cpuUsage);
        if (sumSize >= 0.8 * sumCapacity && cpuUsage > 0 && cpuUsage < 0.7) {
            addConsumer();
        } else if (sumSize <= 0.1 * sumCapacity && consumers.size() > coreConsumers) {
            removeConsumer();
        }
    }

    // thread-unsafe
    private void optimizeQueueSize() {
        int remainCapacity = BoltExecutorOptimizerUtil.getRemainCapacityBaseOnMemory();
        if (remainCapacity <= 0) {
            return;
        }
        System.out.println("optimize queue size...");
    }

    // thread-unsafe
    private void optimizeWorkers() {
        System.out.println("optimize workers...");

    }
}
