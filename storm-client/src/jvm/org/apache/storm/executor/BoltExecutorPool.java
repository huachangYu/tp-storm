package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.storm.executor.bolt.BoltExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoltExecutorPool {
    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutorPool.class);
    private static final double eps = 1e-6;

    private class BoltWorker extends Thread {
        BoltWorker(String threadName) {
            super(threadName);
        }

        @Override
        public void run() {
            while (running) {
                FutureTask<?> task = null;
                try {
                    task = getTask();
                } catch (InterruptedException e) {
                    LOG.warn("error occurred when getting task. ex:{}", e.getMessage());
                }
                if (task != null) {
                    task.run();
                }
            }
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptyThreadWait = lock.newCondition();
    private final Condition emptyQueueWait = lock.newCondition();
    private int coreThreads = 16;
    private final List<BoltExecutor> threads = new ArrayList<>();
    private final List<BoltWorker> workers;
    private final ConcurrentHashMap<String, BlockingQueue<FutureTask<?>>> taskQueues;
    private volatile boolean running = false;
    private final AtomicInteger waitingThreads = new AtomicInteger(0);

    public BoltExecutorPool() {
        this(16);
    }

    public BoltExecutorPool(int coreThreads) {
        this.coreThreads = coreThreads;
        this.taskQueues = new ConcurrentHashMap<>(coreThreads);
        this.workers = new ArrayList<>(coreThreads);
        this.running = true;
    }

    public FutureTask<?> getTask() throws InterruptedException {
        lock.lock();
        try {
            List<BoltExecutor> notEmptyThreads = threads.stream()
                    .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                waitingThreads.addAndGet(1);
                emptyQueueWait.await();
                waitingThreads.addAndGet(-1);
                notEmptyThreads = threads.stream()
                        .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            }

            final long current = System.currentTimeMillis();
            final int minTaskQueueSize = notEmptyThreads.stream().mapToInt(t -> taskQueues.get(t.getName()).size()).min().getAsInt();
            final int maxTaskQueueSize = notEmptyThreads.stream().mapToInt(t -> taskQueues.get(t.getName()).size()).max().getAsInt();
            final double minAvgTime = notEmptyThreads.stream().mapToDouble(t -> t.getMonitor().getAvgTime()).min().getAsDouble();
            final double maxAvgTime = notEmptyThreads.stream().mapToDouble(t -> t.getMonitor().getAvgTime()).max().getAsDouble();
            final long minWaitingTime = notEmptyThreads.stream().mapToLong(t -> t.getMonitor().getWaitingTime(current)).min().getAsLong();
            final long maxWaitingTime = notEmptyThreads.stream().mapToLong(t -> t.getMonitor().getWaitingTime(current)).max().getAsLong();
            for (BoltExecutor boltExecutor : notEmptyThreads) {
                boltExecutor.getMonitor().calculateWeight(current, taskQueues.get(boltExecutor.getName()).size(),
                        minTaskQueueSize, maxTaskQueueSize, minAvgTime, maxAvgTime, minWaitingTime, maxWaitingTime);
            }
            Collections.shuffle(notEmptyThreads);
            BoltExecutor maxQueueSizeThread = Collections.max(notEmptyThreads, (a, b) -> {
                if (Math.abs(b.getWeight() - a.getWeight()) < eps) {
                    return 0;
                }
                return b.getWeight() - a.getWeight() > 0 ? 1 : -1;
            });
            return taskQueues.get(maxQueueSizeThread.getName()).poll();
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
            taskQueues.put(threadName, new LinkedBlockingQueue<>());
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

    public void submit(String threadName, FutureTask<?> futureTask) {
        lock.lock();
        try {
            taskQueues.get(threadName).add(futureTask);
            if (workers.size() < coreThreads) {
                addWorker();
            }
            if (waitingThreads.get() > 0) {
                emptyQueueWait.signal();
            }
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
                worker.start();
            }
        } finally {
            lock.unlock();
        }
    }
}
