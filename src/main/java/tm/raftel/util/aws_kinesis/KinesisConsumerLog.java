package tm.raftel.util.aws_kinesis;

public class KinesisConsumerLog {

    public String streamName;
    public String consumerName;
    public String[] others;

    public KinesisConsumerLog(KinesisConsumerBag kinesisConsumerBag, String... others) {
        this.streamName = kinesisConsumerBag.getStreamName();
        this.consumerName = kinesisConsumerBag.getConsumerName();
        this.others = others;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(String.format("%s, ", streamName));
        sb.append(String.format("%s", consumerName));
        for (String other : others) {
            sb.append(", ");
            sb.append(String.format("%s", other));
        }
        sb.append("]");
        return sb.toString();
    }
}
