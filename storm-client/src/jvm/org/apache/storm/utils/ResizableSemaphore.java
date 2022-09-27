package org.apache.storm.utils;

import java.util.concurrent.Semaphore;

/**
 * Copy from <a href="https://github.com/OpenRock/OpenAM">openAm</a>.
 * */
public class ResizableSemaphore extends Semaphore {
    public ResizableSemaphore(int permits, boolean fair) {
        super(permits, fair);
    }

    public ResizableSemaphore(int permits) {
        super(permits);
    }

    @Override
    protected void reducePermits(int reduction) {
        super.reducePermits(reduction);
    }

    public void increasePermits(int increase) {
        this.release(increase);
    }
}
