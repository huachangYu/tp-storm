package org.apache.storm.executor.strategy;

public abstract class ScheduleStrategy implements IScheduleStrategy {
    protected int status = 0; // 0: low load, 1: high load

    public void setStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
