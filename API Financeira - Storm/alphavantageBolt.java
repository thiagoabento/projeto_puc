package org.example;


import com.esotericsoftware.minlog.Log;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class alphavantageBolt extends BaseBasicBolt{

    public void prepare(Map stormConf,TopologyContext context){}

    public void execute(Tuple input, BasicOutputCollector collector) {

        try {
            String symbol = input.getStringByField("symbol");
            Double price = Double.parseDouble(input.getValueByField("price").toString());
            Double previousClose = Double.parseDouble(input.getValueByField("previousClose").toString());
            Boolean ganho = price >= previousClose;

            System.out.println("Empresa: " + symbol);
            System.out.println("Valor atual da ação: " + price);
            System.out.println("Valor da cotação no dia anterior: " + previousClose);

            if (ganho) {
                System.out.println("Houve ganho na cotação!");
            } else {
                System.out.println("Houve perda em comparação ao dia anterior!");
            }

            collector.emit(new Values(symbol, price, previousClose));

        }catch (Exception e){
            e.printStackTrace();
            collector.reportError(e);
        }
    }
    public void declareOutputFields (OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("symbol","price","previousClose"));
    }

    public void cleanup () {
        System.out.println("Finalizando sessão bolt....");
    }

}
