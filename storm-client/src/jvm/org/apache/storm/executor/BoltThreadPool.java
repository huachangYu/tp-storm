package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.executor.bolt.BoltThread;
import org.apache.storm.shade.io.netty.util.internal.ConcurrentSet;

public class BoltThreadPool {
    private int maxThreadNum = Integer.MAX_VALUE;
    private final List<BoltThread> threads = new ArrayList<>();
    private final ConcurrentSet<UUID> threadWaitingList = new ConcurrentSet<>();

    private volatile boolean ready = false;

    private long lastOptimizeTime = 0;

    private long optimizeTimeGap = 10;
    public BoltThreadPool() {
        this(Integer.MAX_VALUE);
    }

    public BoltThreadPool(int coreNum) {
        this.maxThreadNum = coreNum;
    }

    public synchronized void optimize() {
        if (!ready) {
            return;
        }
        Collections.shuffle(threads);
        threads.sort((a, b) -> b.getWeight() - a.getWeight());
        for (int i = 0; i < threads.size(); i++) {
            UUID uuid = threads.get(i).getUuid();
            if (i < maxThreadNum) {
                if (threadWaitingList.contains(uuid)) {
                    threadWaitingList.remove(uuid);
                    System.out.printf("thread [%s] wakeup\n", threads.get(i).getName());
                    LockSupport.unpark(threads.get(i));
                }
            } else {
                threadWaitingList.add(uuid);
            }
        }
    }

    public synchronized void setReady(boolean ready) {
        this.ready = ready;
    }

    public synchronized void addThread(BoltThread thread) {
        threads.add(thread);
        thread.setInThreadPool(true);
        ReentrantLock lock = new ReentrantLock();
        if (threads.size() - threadWaitingList.size() >= maxThreadNum) {
            threadWaitingList.add(thread.getUuid());
        }
    }

    public synchronized void addThreads(List<BoltThread> threads) {
        for (BoltThread thread : threads) {
            addThread(thread);
        }
    }

    public int getMaxThreadNum() {
        return maxThreadNum;
    }

    public synchronized void setMaxThreadNum(int maxThreadNum) {
        this.maxThreadNum = maxThreadNum;
    }

    public synchronized void removeThread(UUID uuid) {
        threadWaitingList.removeIf(t -> t.equals(uuid));
        threads.removeIf(t -> t.getUuid().equals(uuid));
    }

    public synchronized BoltThread getThread(UUID uuid) {
        for (BoltThread thread : threads) {
            if (thread.getUuid().equals(uuid)) {
                return thread;
            }
        }
        return null;
    }

    public synchronized boolean shouldWaiting(UUID uuid) {
        return threadWaitingList.contains(uuid);
    }

    public synchronized boolean shouldOptimize() {
        if (!ready) {
            return false;
        }
        boolean isAllZero = true;
        for (BoltThread thread : threads) {
            if (thread.getWeight() != 0) {
                isAllZero = false;
                break;
            }
        }
        if (isAllZero) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastOptimizeTime > optimizeTimeGap) {
            lastOptimizeTime = currentTime;
            return true;
        }
        return false;
    }
}
