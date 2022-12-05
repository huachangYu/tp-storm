/**
 * for debug
 * remove it when releasing
 * */

package org.apache.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountTopology {
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector collector;
        Random rand;

        long preTime = System.currentTimeMillis();
        int count = 0;

        private static final String[] sentences = new String[]{
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature" };

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            rand = new Random();
        }

        @Override
        public void nextTuple() {
            long current = System.currentTimeMillis();
            if (current - preTime >= 1000) {
                System.out.printf("spout qps=%d\n", count);
                count = 0;
                preTime = current;
            }
            count++;
            String sentence = sentences[rand.nextInt(sentences.length)];
            collector.emit(new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            String[] words = sentence.split(" ");
            for (String w : words) {
                collector.emit(new Values(w));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Long> counts = new HashMap<String, Long>();

        @SuppressWarnings("checkstyle:WhitespaceAfter")
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count += 1;
            counts.put(word, count);
            // System.out.printf("word=%s, num=%d\n", word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        //conf.setNumWorkers(2);
        conf.useExecutorPool(true);
        conf.setExecutorPoolCoreConsumers(1);
        conf.setExecutorPoolMaxConsumers(4);
        conf.setExecutorPoolStrategy("ROUND_ROBIN");
        conf.setExecutorPoolTotalQueueCapacity(2000000);
        conf.enableWorkersOptimize(true);
        conf.enableExecutorPoolOptimize(true);
        conf.setExecutorPoolIds(Arrays.asList("split", "count"));
        conf.enableExecutorPoolPrintMetrics(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
    }
}