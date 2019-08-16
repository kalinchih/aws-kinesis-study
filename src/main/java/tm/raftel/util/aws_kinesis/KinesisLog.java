package tm.raftel.util.aws_kinesis;

public class KinesisLog {

    public String streamName;
    public String consumerName;
    public String[] others;

    public KinesisLog(KinesisConsumerConfig kinesisConsumerConfig, String... others) {
        this.streamName = kinesisConsumerConfig.getStreamName();
        this.consumerName = kinesisConsumerConfig.getConsumerName();
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
