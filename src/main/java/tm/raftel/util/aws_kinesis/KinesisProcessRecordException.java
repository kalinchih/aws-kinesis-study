package tm.raftel.util.aws_kinesis;

public class KinesisProcessRecordException extends Exception {

    private static final long serialVersionUID = 1L;

    public KinesisProcessRecordException(KinesisLog kinesisLog, Throwable cause) {
        super(String.format("Fail to process record. KinesisConsumerLog=%s.", kinesisLog), cause);
    }
}
