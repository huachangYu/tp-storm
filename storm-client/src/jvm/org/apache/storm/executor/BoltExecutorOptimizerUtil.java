package org.apache.storm.executor;

public class BoltExecutorOptimizerUtil {

    public static int getRemainCapacityBaseOnMemory() {
        // must be bigger than sizeof(BoltTask)
        final long itemSize = 200;
        //leave 1.5GB of memory for OS
        final long freeMem = Runtime.getRuntime().freeMemory() - (long) (1.5 * 1024 * 1024);
        return (int) Math.max(0, freeMem / itemSize);
    }



}
