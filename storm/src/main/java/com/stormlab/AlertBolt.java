package com.stormlab;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class AlertBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String user = input.getStringByField("user");
        long amount = input.getLongByField("amount");
        System.out.println("ALERT: High Value Transaction detected! User: " + user + " Amount: $" + amount);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
