package Kafka.Storm.Sentimental;

/**
 * Created by hastimal on 9/29/2015.
 * Reference : https://github.com/shekhargulati/day20-stanford-sentiment-analysis-demo
 */
public class TweetWithSentiment {

    private String line;
    private String cssClass;

    public TweetWithSentiment() {
    }

    public TweetWithSentiment(String line, String cssClass) {
        super();
        this.line = line;
        this.cssClass = cssClass;
    }

    public String getLine() {
        return line;
    }

    public String getCssClass() {
        return cssClass;
    }

    @Override
    public String toString() {
        return "Kafka.Storm.Sentimental.TweetWithSentiment [line=" + line + ", cssClass=" + cssClass + "]";
    }

}