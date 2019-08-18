package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.model.Record;

public interface KinesisConsumerHelper {

    public void handleRecord(Record record, String shardId) throws Exception;

    public void alertConsumerRestart();
}
