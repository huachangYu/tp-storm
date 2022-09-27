package org.apache.storm.utils;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Copy from <a href="https://github.com/OpenRock/OpenAM">openAm</a>.
 * */
public class ResizableLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private int queueSize;
    private final ResizableSemaphore availablePlaces;

    public ResizableLinkedBlockingQueue() {
        super();
        queueSize = Integer.MAX_VALUE;
        availablePlaces = new ResizableSemaphore(queueSize, true);
    }

    public ResizableLinkedBlockingQueue(Collection<? extends E> c) {
        super(c);
        queueSize = Integer.MAX_VALUE;
        availablePlaces = new ResizableSemaphore(queueSize, true);
    }

    public ResizableLinkedBlockingQueue(int initialCapacity) {
        super();
        queueSize = initialCapacity;
        availablePlaces = new ResizableSemaphore(initialCapacity, true);
    }

    public synchronized void resizeQueue(int newSize) throws IllegalArgumentException {
        if (newSize < 0) {
            throw new IllegalArgumentException("Cannot set queue size to a value below zero.");
        }
        int difference;
        if (newSize < queueSize) {
            difference = queueSize - newSize;
            availablePlaces.reducePermits(difference);
        } else if (newSize > queueSize) {
            difference = newSize - queueSize;
            availablePlaces.increasePermits(difference);
        }
        queueSize = newSize;
    }

    public int getMaximumQueueSize() {
        return queueSize;
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        boolean returnValue;
        if (availablePlaces.tryAcquire()) {
            if (super.offer(e)) {
                returnValue = true;
            } else {
                returnValue = false;
                availablePlaces.release();
            }
        } else {
            returnValue = false;
        }
        return returnValue;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null) {
            return false;
        }
        boolean returnValue;
        if (availablePlaces.tryAcquire(timeout, unit)) {
            if (super.offer(e)) {
                returnValue = true;
            } else {
                returnValue = false;
                availablePlaces.release();
            }
        } else {
            returnValue = false;
        }
        return returnValue;
    }

    @Override
    public E poll() {
        E returnValue = super.poll();
        if (returnValue != null) {
            availablePlaces.release();
        }
        return returnValue;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E resultValue = super.poll(timeout, unit);
        if (resultValue != null) {
            availablePlaces.release();
        }
        return resultValue;
    }

    @Override
    public void put(E e) throws InterruptedException {
        availablePlaces.acquire();
        super.put(e);
    }

    @Override
    public int remainingCapacity() {
        return availablePlaces.availablePermits();
    }

    @Override
    public boolean remove(Object o) {
        if (super.remove(o)) {
            availablePlaces.release();
            return true;
        }
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        E result = super.take();
        availablePlaces.release();
        return result;
    }
}