package com.stormlab;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TransactionFilterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String jsonString = input.getStringByField("value");
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(jsonString);
            
            // JSON-simple parses numbers as Long
            long amount = (Long) json.get("amount");
            String user = (String) json.get("user");

            if (amount > 500) {
                collector.emit(new Values(user, amount));
            }
        } catch (Exception e) {
            // Ignore parse errors
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "amount"));
    }
}
