package org.example;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;

public class alphavantageSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final String API_KEY = "UE1LBQU74PFKDIII";
    private static final String SYMBOL = "GOOG";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            String urlStr = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + SYMBOL + "&apikey=" + API_KEY;
            URL url = new URL(urlStr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(10000);

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder jsonBuilder = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                jsonBuilder.append(line);
            }

            reader.close();
            connection.disconnect();

            String response = jsonBuilder.toString();
            JsonNode root = MAPPER.readTree(response);

            // Analisa se a API bloqueou a consulta aos dados
            if (root.has("Note")) {
                Log.warn("Aviso da API: " + root.get("Note").asText());
                Thread.sleep(60000); // Aguarda 1 minuto
                return;
            }

            JsonNode quote = root.get("Global Quote");
            if (quote == null || quote.isEmpty()) {
                Log.warn(" Nenhum dado encontrado para a cotação.");
                Thread.sleep(30000); // Aguarda para consultar novamente
                return;
            }

            double price = Double.parseDouble(quote.get("05. price").asText());
            double previousClose = Double.parseDouble(quote.get("08. previous close").asText());

            collector.emit(new Values(SYMBOL, price, previousClose));
            Log.info("📊 Cotação emitida: " + SYMBOL + " | Atual: " + price + " | Anterior: " + previousClose);

        } catch (Exception e) {
            Log.warn("❗ Erro ao buscar cotação: " + e.getMessage());
        }

        // Tempo de espera entre consultas
        try {
            Thread.sleep(60000); // 1 minuto entre chamadas
        } catch (InterruptedException ignored) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("symbol", "price", "previousClose"));
    }
}
