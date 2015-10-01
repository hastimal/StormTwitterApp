import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * Created by hastimal on 9/29/2015.
 */
public class TwitterSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String _username;
    String _pwd;
    String consumer_key="t0tAnvsGPStnvRJe6LPOaIjLo";
    String consumer_secret="tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0";
    String access_token="1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez";
    String token_secret="GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty";
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Status"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = spoutOutputCollector;
        StatusListener listener = new StatusListener() {

            public void onException(Exception e) {

            }

            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        TwitterStreamFactory fact = new TwitterStreamFactory();
        _twitterStream = fact.getInstance();
        _twitterStream.setOAuthConsumer(consumer_key, consumer_secret);
        _twitterStream.setOAuthAccessToken(new AccessToken(access_token, token_secret));
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            String s=ret.getText();
            // emit tuple to next bolt
            _collector.emit(new Values(s));
        }
    }
}
