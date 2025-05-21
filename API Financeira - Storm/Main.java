package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;

public class Main{

    public static void main(String[] args) throws InterruptedException, TException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("alphavantageSpout", new alphavantageSpout());
        builder.setBolt("alphavantageBolt", new alphavantageBolt()).shuffleGrouping("alphavantageSpout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = null;
        try {
            cluster = new LocalCluster();
            cluster.submitTopology("TopologiaAlphaVantage", config,builder.createTopology());
            Thread.sleep(50000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {

    }finally {
            if (cluster != null) {
              //  cluster.shutdown();
            }
            cluster.shutdown();
        }
    }
}