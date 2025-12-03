package com.stormlab;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class LabTopology {
    public static void main(String[] args) throws Exception {
        
        // Connect to "kafka" because this code runs INSIDE the docker network
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig
            .builder("kafka:29092", "transactions")
            .setProp("group.id", "storm-group")
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
            .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout<>(spoutConfig), 1);
        builder.setBolt("filter-bolt", new TransactionFilterBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("alert-bolt", new AlertBolt(), 1).shuffleGrouping("filter-bolt");

        Config config = new Config();
        config.setNumWorkers(2);

        // Submit to the cluster
        StormSubmitter.submitTopology("fraud-detector", config, builder.createTopology());
    }
}
