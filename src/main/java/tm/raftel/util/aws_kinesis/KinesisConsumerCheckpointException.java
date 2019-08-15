package tm.raftel.util.aws_kinesis;

public class KinesisConsumerCheckpointException extends Exception {

    private static final long serialVersionUID = 1L;

    public KinesisConsumerCheckpointException(String message, KinesisConsumerLog kinesisConsumerLog, Throwable cause) {
        super(String.format("Fail to checkpoint. Message=%s. KinesisConsumerLog=%s.", message, kinesisConsumerLog), cause);
    }
}
