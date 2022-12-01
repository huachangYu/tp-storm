package org.apache.storm.utils;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ResizableBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private int capacity;
    private final ResizableSemaphore availablePlaces;

    public ResizableBlockingQueue() {
        super();
        capacity = Integer.MAX_VALUE;
        availablePlaces = new ResizableSemaphore(capacity, false);
    }

    public ResizableBlockingQueue(Collection<? extends E> c) {
        super(c);
        capacity = Integer.MAX_VALUE;
        availablePlaces = new ResizableSemaphore(capacity, false);
    }

    public ResizableBlockingQueue(int initialCapacity) {
        super();
        capacity = initialCapacity;
        availablePlaces = new ResizableSemaphore(initialCapacity, true);
    }

    public synchronized void resizeQueue(int newSize) throws IllegalArgumentException {
        if (newSize < 0) {
            throw new IllegalArgumentException("Cannot set queue size to a value below zero.");
        }
        int difference;
        if (newSize < capacity) {
            difference = capacity - newSize;
            availablePlaces.reducePermits(difference);
        } else if (newSize > capacity) {
            difference = newSize - capacity;
            availablePlaces.increasePermits(difference);
        }
        capacity = newSize;
    }

    public int getCapacity() {
        return capacity;
    }

    public double getLoad() {
        return (double) size() / (double) Math.max(1, getCapacity());
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
            throw new NullPointerException();
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