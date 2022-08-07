package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
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
            while (true) {
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
    private final int coreNum;
    private final List<BoltExecutor> threads = new ArrayList<>();
    private final List<BoltWorker> workers;
    private final ConcurrentHashMap<String, BlockingQueue<FutureTask<?>>> taskQueues;

    public BoltExecutorPool(int coreNum) {
        this.coreNum = coreNum;
        this.taskQueues = new ConcurrentHashMap<>(coreNum);
        this.workers = new ArrayList<>(coreNum);
    }

    public FutureTask<?> getTask() throws InterruptedException {
        lock.lock();
        try {
            if (taskQueues.size() == 0) {
                emptyThreadWait.await();
            }
            List<BoltExecutor> notEmptyThreads = threads.stream()
                    .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            while (notEmptyThreads.isEmpty()) {
                emptyQueueWait.await();
                notEmptyThreads = threads.stream()
                        .filter(t -> taskQueues.get(t.getName()).size() > 0).collect(Collectors.toList());
            }

            Collections.shuffle(notEmptyThreads);
            BoltExecutor maxQueueSizeThread = Collections.max(notEmptyThreads, (a, b) -> {
                if (Math.abs(b.getWeight() - a.getWeight()) < eps) {
                    return taskQueues.get(b.getName()).size() - taskQueues.get(a.getName()).size();
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
        taskQueues.remove(threadName);
        threads.removeIf(t -> t.getName().equals(threadName));
        lock.unlock();
    }

    public void shutdown() {
        for (BoltWorker worker : workers) {
            worker.interrupt();
        }
    }

    public void submit(String threadName, FutureTask<?> futureTask) {
        lock.lock();
        taskQueues.get(threadName).add(futureTask);
        if (workers.size() < coreNum) {
            addWorker();
        }
        emptyQueueWait.signal();
        lock.unlock();
    }

    private void addWorker() {
        synchronized (workers) {
            if (workers.size() < coreNum) {
                BoltWorker worker = new BoltWorker("bolt-worker-" + workers.size());
                workers.add(worker);
                worker.start();
            }
        }
    }
}
