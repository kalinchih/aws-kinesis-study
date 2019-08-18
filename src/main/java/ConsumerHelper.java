import com.amazonaws.services.kinesis.model.Record;
import k0.util.aws_kinesis.KinesisConsumerHelper;
import k0.util.config.ConfigUtils;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

public class ConsumerHelper implements KinesisConsumerHelper {

    private CharsetDecoder recordDataDecoderCharset;

    public ConsumerHelper() throws Exception {
        ConfigUtils configUtils = ConfigUtils.build();
        Properties config = configUtils.getProperties(String.format("config.properties"));
        recordDataDecoderCharset = Charset.forName(configUtils.getProperty(config, "recordDataDecoder")).newDecoder();
    }

    @Override
    public void handleRecord(Record record, String shardId) throws Exception {
        //        if (1 == 1) {
        //            String message = "OMG! OMG! GG when processing a stream record!!!";
        //            System.err.println(message);
        //            throw new Exception(message);
        //        }
        String recordData = recordDataDecoderCharset.decode(record.getData()).toString();
        System.out.println(String.format("Processed recordData=%s, sequenceNumber=%s.", recordData,
                record.getSequenceNumber()));
    }

    @Override
    public void alertConsumerRestart() {
        String message = "OMG! OMG! KinesisConsumer restart!!!";
        System.err.println(message);
        System.err.println(new Exception(message));
    }
}
