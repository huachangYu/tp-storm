package org.apache.storm.executor;

public class BoltExecutorOptimizerUtil {

    public static int getMaxCapacityBaseOnMemory() {
        final long itemSize = 200; // must be bigger than sizeof(BoltTask)
        final long freeMem = Runtime.getRuntime().freeMemory() - 1024 * 1024; //leave 1GB of memory for OS
        return (int) Math.max(0, freeMem / itemSize);
    }


}
