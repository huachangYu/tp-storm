package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.storm.executor.bolt.BoltThread;
import org.apache.storm.shade.io.netty.util.internal.ConcurrentSet;

public class BoltThreadPool implements Runnable {
    private int maxThreadNum = Integer.MAX_VALUE;
    private final List<BoltThread> boltThreads = new ArrayList<>();
    private final ConcurrentSet<UUID> threadswaiting = new ConcurrentSet<>();
    private volatile boolean running = true;

    public BoltThreadPool() {

    }

    public BoltThreadPool(int coreNum) {
        this.maxThreadNum = coreNum;
    }

    @Override
    public void run() {
        while (running) {
            Collections.shuffle(boltThreads);
            boltThreads.sort((a, b) -> b.getWeight() - a.getWeight());
            for (int i = 0; i < boltThreads.size(); i++) {
                UUID uuid = boltThreads.get(i).getUuid();
                if (i < maxThreadNum) {
                    threadswaiting.remove(uuid);
                    System.out.printf("thread [%s] wakeup\n", uuid);
                    LockSupport.unpark(boltThreads.get(i));
                } else {
                    threadswaiting.add(uuid);
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void stop() {
        running = false;
    }

    public void addThread(BoltThread thread) {
        boltThreads.add(thread);
        thread.setInThreadPool(true);
        ReentrantLock lock = new ReentrantLock();
        if (boltThreads.size() - threadswaiting.size() >= maxThreadNum) {
            threadswaiting.add(thread.getUuid());
        }
    }

    public void addThreads(List<BoltThread> threads) {
        for (BoltThread thread : threads) {
            addThread(thread);
        }
    }

    public int getMaxThreadNum() {
        return maxThreadNum;
    }

    public void setMaxThreadNum(int maxThreadNum) {
        this.maxThreadNum = maxThreadNum;
    }

    public void removeThread(UUID uuid) {

    }

    public boolean shouldWaiting(UUID uuid) {
        return threadswaiting.contains(uuid);
    }
}
