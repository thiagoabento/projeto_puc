package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpConnection;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;


public class alphavantageSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static String api_alphavantage = "UE1LBQU74PFKDIII";
    private static String symbol = "GOOG";


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        try {
            String url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + symbol + "&apikey=" + api_alphavantage + "";
            URL urlAlphaVantage = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) urlAlphaVantage.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(10000);

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder stringBuilder = new StringBuilder();
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);

            }

            bufferedReader.close();
            connection.disconnect();

            String response = stringBuilder.toString();
            JsonNode root = new ObjectMapper().readTree(response);
            JsonNode quoteAlpha = root.get("Global Quote");

            if (quoteAlpha != null) {
                // Extrair o preço da cotação e emiti-lo junto com o símbolo
                double price = Double.parseDouble(quoteAlpha.get("05. price").asText()); // Preço da ação
                double previousClose = Double.parseDouble(quoteAlpha.get("08. previous close").asText()); // Preço da ação dia anterior

                collector.emit(new Values(symbol, price,previousClose));  // Pegando simbolo, preço atual e preço dia anterior
            }


        } catch (Exception a) {
            System.out.println("Erro ao buscar cotação: " + a.getMessage());
        }
        try {
            Thread.sleep(60000);
        } catch (InterruptedException ignored) {
        }

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("symbol","price","previousClose"));
    }

}
