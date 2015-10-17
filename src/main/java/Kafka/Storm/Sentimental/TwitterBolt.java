package Kafka.Storm.Sentimental;

import Kafka.Storm.Sentimental.SentimentAnalyzer;
import Kafka.Storm.Sentimental.TweetWithSentiment;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
/**
 * Created by hastimal on 9/29/2015.
 */
public class TwitterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String s= tuple.getStringByField("Status");
        SentimentAnalyzer sa=new SentimentAnalyzer();
        TweetWithSentiment tw=sa.findSentiment(s);
        insertIntoMongoDB(tw.getLine(),tw.getCssClass());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
//Zzl0OZUNbG988MQrWklonTyiuqYg90YA
    public static void insertIntoMongoDB(String tweet, String sentiment) {
        try {
           // URL url = new URL("https://api.mongolab.com/api/1/databases/cs5543/collections/TwitterSentiment?apiKey=APIKEY");
            //https://api.mongolab.com/api/1/databases?apiKey=2E81PUmPFI84t7UIc_5YdldAp1ruUPKye
            URL url = new URL("https://api.mongolab.com/api/1/databases/twiiterdb/collections/cs5543?apiKey=Zzl0OZUNbG988MQrWklonTyiuqYg90YA");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            String input = "{\"tweet\":\"" + tweet + "\",\"sentiment\":\"" + sentiment + "\",\"time\":\"" + System.currentTimeMillis() + "\"}";

            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                System.out.println("The code is " + conn.getResponseMessage());
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }

            conn.disconnect();
        } catch (Exception e) {

            e.printStackTrace();

        }

    }
}