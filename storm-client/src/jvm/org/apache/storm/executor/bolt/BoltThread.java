package org.apache.storm.executor.bolt;

import java.util.UUID;

import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Utils;

public class BoltThread extends Utils.SmartThread {
    private UUID uuid;
    private JCQueue receiveQueue;
    private boolean inThreadPool = false;

    public BoltThread(Runnable r,  UUID uuid) {
        super(r);
        this.uuid = uuid;
    }

    public void setReceiveQueue(JCQueue receiveQueue) {
        this.receiveQueue = receiveQueue;
    }

    public void setInThreadPool(boolean inThreadPool) {
        this.inThreadPool = inThreadPool;
    }

    public boolean isInThreadPool() {
        return inThreadPool;
    }

    public int getWeight() {
        return receiveQueue.size();
    }

    public UUID getUuid() {
        return uuid;
    }
}
