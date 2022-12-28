# TP-Storm

# what is new
The TP-Storm is based on [Apache Storm v2.4.0](https://storm.apache.org/). New features are as follows:
- Better performance
- Automatically adjust parameters according to system status
# Implementation details
todo
# How to Use
WordCount topology is as follows:
```java
public static void main(String[] args) throws Exception {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("spout", new RandomSentenceSpout(), 1);
      builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
      builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));

      Config conf = new Config();
      conf.useExecutorPool(true);
      conf.setExecutorPoolCoreConsumers(1);
      conf.setExecutorPoolMaxConsumers(4);
      conf.setExecutorPoolStrategy("AD");
      conf.setExecutorPoolTotalQueueCapacity(2000000);
      conf.enableWorkersOptimize(true);
      conf.enableExecutorPoolOptimize(true);
      conf.setExecutorPoolIds(Arrays.asList("split", "count"));
      conf.enableExecutorPoolPrintMetrics(true);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());
  }
```
# Committers
XXX
