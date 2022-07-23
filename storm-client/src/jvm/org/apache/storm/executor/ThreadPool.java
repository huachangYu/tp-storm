package org.apache.storm.executor;

public class ThreadPool {
    private boolean running = false;

    public void start() {
        running = true;
    }

    public void stop() {
        running = false;
        //TODO notify all threads

    }
    //TODO
}
