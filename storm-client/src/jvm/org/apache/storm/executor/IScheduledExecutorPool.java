package org.apache.storm.executor;

import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.executor.bolt.BoltExecutor;

public interface IScheduledExecutorPool extends Shutdownable {
    void addThread(BoltExecutor thread);

    void removeThread(String threadName);

    void submit(String threadName, BoltTask task);
}
