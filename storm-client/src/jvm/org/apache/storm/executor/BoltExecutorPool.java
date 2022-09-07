package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.storm.executor.bolt.BoltExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltExecutorPool {
    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutorPool.class);
    private static final double EPS = 1e-5;
    private static final int MAX_QUEUE_CAPACITY = 100000;

    private class BoltWorker extends Thread {
        BoltWorker(String threadName) {
            super(threadName);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    List<BoltTask> tasks = getTask(maxTasks);
                    for (BoltTask task : tasks) {
                        long start = task.isNeedToRecord() ? System.currentTimeMillis() : 0;
                        task.run();
                        if (start > 0) {
                            long end = System.currentTimeMillis();
                            task.getMonitor().record(end - start);
                        }
                    }
                    if (tasks.size() > 0) {
                        tasks.get(0).getMonitor().recordLastTime(System.currentTimeMillis());
                    }
                } catch (InterruptedException e) {
                    LOG.warn("error occurred when getting task. ex:{}", e.getMessage());
                }
            }
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptyThreadWait = lock.newCondition();
    private final Condition emptyQueueWait = lock.newCondition();
    private final int coreThreads;
    private final List<BoltExecutor> threads = new ArrayList<>();
    private final List<BoltWorker> workers;
    private final ConcurrentHashMap<String, BlockingQueue<BoltTask>> taskQueues;
    private final AtomicInteger totalTaskCount;
    private boolean running; // to improve performance, don't use violate
    private final int maxTasks;

    public BoltExecutorPool() {
        this(16, 1);
    }

    public BoltExecutorPool(int coreThreads, int maxTasks) {
        this.coreThreads = coreThreads;
        this.taskQueues = new ConcurrentHashMap<>(coreThreads);
        this.totalTaskCount = new AtomicInteger(0);
        this.workers = new ArrayList<>(coreThreads);
        this.running = true;
        this.maxTasks = maxTasks;
        while (workers.size() < coreThreads) {
            addWorker();
        }
    }

    public List<BoltTask> getTask(int maxTaskSize) throws InterruptedException {
        lock.lock();
        try {
            List<BoltExecutor> notEmptyThreads = threads.stream()
                    .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                emptyQueueWait.await();
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
                totalTaskCount.getAndDecrement();
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
            taskQueues.put(threadName, new LinkedBlockingQueue<>(MAX_QUEUE_CAPACITY));
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
        this.running = false;
        for (BoltWorker worker : workers) {
            worker.interrupt();
        }
    }

    public void submit(String threadName, BoltTask futureTask) throws InterruptedException {
        taskQueues.get(threadName).put(futureTask);
        int c = totalTaskCount.getAndIncrement();
        if (c == 0) {
            signalNotEmpty();
        }
    }

    private void signalNotEmpty() {
        lock.lock();
        try {
            emptyQueueWait.signal();
        } finally {
            lock.unlock();
        }
    }

    private void addWorker() {
        lock.lock();
        try {
            if (workers.size() < coreThreads) {
                BoltWorker worker = new BoltWorker("bolt-worker-" + workers.size());
                workers.add(worker);
                worker.setPriority(Thread.MAX_PRIORITY);
                worker.setDaemon(true);
                worker.start();
            }
        } finally {
            lock.unlock();
        }
    }
}
