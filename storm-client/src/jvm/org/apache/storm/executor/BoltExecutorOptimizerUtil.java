package org.apache.storm.executor;

import com.github.signaflo.timeseries.TimeSeries;
import com.github.signaflo.timeseries.model.arima.Arima;
import com.github.signaflo.timeseries.model.arima.ArimaOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.bolt.BoltExecutorMonitor;
import org.apache.storm.utils.ResizableLinkedBlockingQueue;

public class BoltExecutorOptimizerUtil {
    private static int MAX_TOTAL_CAPACITY  = getRemainCapacityBaseOnMemory();

    public static void setMaxTotalCapacity(int capacity) {
        MAX_TOTAL_CAPACITY = capacity;
    }

    public static int getRemainCapacityBaseOnMemory() {
        // must be bigger than sizeof(BoltTask)
        final long itemSize = 80;
        //leave 1GB of memory for OS
        final long freeMem = Runtime.getRuntime().freeMemory() - (long) (1.5 * 1024 * 1024);
        return (int) Math.max(0, freeMem / itemSize);
    }

    public static Arima buildArima(double[] x, double[] y, double deltaX) {
        if (x.length != y.length) {
            throw new IllegalArgumentException("x and y must be of the same size.");
        }
        SplineInterpolator interpolator = new SplineInterpolator();
        PolynomialSplineFunction function = interpolator.interpolate(x, y);
        List<Double> data = new ArrayList<>();
        for (double curX = x[0]; curX <= x[x.length - 1]; curX += deltaX) {
            data.add(function.value(curX));
        }
        double[] trainY = data.stream().mapToDouble(t -> t).toArray();
        TimeSeries series = TimeSeries.from(trainY);
        return Arima.model(series, ArimaOrder.order(2, 1, 1));
    }

    public static Map<String, Integer> getIncreaseBaseOnArima(Map<String, ResizableLinkedBlockingQueue<BoltTask>> taskQueues,
                                                              List<BoltExecutor> bolts,
                                                              int minCapacity,
                                                              int maxCapacity,
                                                              long currentTime) {
        Map<String, Integer> increase = new HashMap<>();
        int remainCapacity = MAX_TOTAL_CAPACITY == -1 ? getRemainCapacityBaseOnMemory() :
                Math.max(0, MAX_TOTAL_CAPACITY - taskQueues.values().stream()
                        .mapToInt(ResizableLinkedBlockingQueue::getMaximumQueueSize).sum());
        if (remainCapacity <= 0) {
            return increase;
        }
        Set<String> ignoreQueueNames = new HashSet<>();
        for (String queueName : taskQueues.keySet()) {
            ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
            if (queue.getMaximumQueueSize() <= minCapacity) {
                continue;
            }
            if (queue.size() < 0.5 * queue.getMaximumQueueSize()) {
                ignoreQueueNames.add(queueName);
                int desc = Math.min(queue.size() - minCapacity, (int) (0.1 * queue.getMaximumQueueSize()));
                remainCapacity += desc;
                increase.put(queueName, -desc);
            } else if (queue.size() >= 0.9 * queue.getMaximumQueueSize()) {
                ignoreQueueNames.add(queueName);
                int incr = Math.min(remainCapacity, (int) (0.1 * queue.getMaximumQueueSize()));
                remainCapacity -= incr;
                increase.put(queueName, incr);
            }
        }
        if (remainCapacity <= 0) {
            return increase;
        }
        Map<String, Double> starts = new HashMap<>();
        Map<String, double[]> preds = new HashMap<>();
        final double delta = 5;
        final long maxTime = currentTime + 1000;
        for (BoltExecutor bolt : bolts) {
            String queueName = bolt.getName();
            if (ignoreQueueNames.contains(queueName)) {
                continue;
            }
            ResizableLinkedBlockingQueue<BoltTask> queue = taskQueues.get(queueName);
            if (queue.remainingCapacity() < 0.1 * queue.getMaximumQueueSize()) {
                int incr =  (int) (0.1 * queue.getMaximumQueueSize());
                increase.put(queueName, incr);
                remainCapacity -= incr;
                ignoreQueueNames.add(queueName);
                continue;
            }
            List<BoltExecutorMonitor.BoltTaskInfo> taskInfos = bolt.getMonitor().getCurrentTaskInfos(currentTime);
            double[] x = taskInfos.stream().mapToDouble(t -> (double) t.getCurrentTime()).toArray();
            double[] y = taskInfos.stream().mapToDouble(t -> (double) t.getCurrentQueueSize()).toArray();
            if (x.length <= 20) {
                ignoreQueueNames.add(queueName);
                continue;
            }
            Arima model = buildArima(x, y, delta);
            int steps = (int) ((maxTime - x[0]) / delta) + 1;
            starts.put(queueName, x[0]);
            preds.put(queueName, model.forecast(steps).pointEstimates().asArray());
        }
        if (preds.size() == 0 || remainCapacity <= 0) {
            return increase;
        }
        for (long cur = currentTime; cur <= maxTime; cur += delta) {
            Map<String, Integer> increaseTmp = new HashMap<>();
            int total = 0;
            for (String queueName : taskQueues.keySet()) {
                if (ignoreQueueNames.contains(queueName)) {
                    continue;
                }
                int step = (int) ((cur - starts.get(queueName)) / delta);
                double predValue = preds.get(queueName)[step];
                int inc = Math.max(0, (int) predValue - taskQueues.get(queueName).getMaximumQueueSize());
                if (inc > 0) {
                    total += inc;
                    increaseTmp.put(queueName, inc);
                }
            }
            if (increaseTmp.size() > 0 || total > remainCapacity) {
                break;
            }
            increase.putAll(increaseTmp);
        }
        return increase;
    }
}
