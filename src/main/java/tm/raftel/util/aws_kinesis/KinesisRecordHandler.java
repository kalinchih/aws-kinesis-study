package tm.raftel.util.aws_kinesis;

public interface KinesisRecordHandler {

    public void handleRecord() throws Exception;
}
