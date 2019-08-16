import com.amazonaws.services.kinesis.model.Record;
import k0.util.aws_kinesis.KinesisConsumerHandler;
import k0.util.config.ConfigUtils;
import k0.util.log.LogUtils;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

public class ConsumerHandler implements KinesisConsumerHandler {

    private CharsetDecoder recordDataDecoderCharset;
    private LogUtils logUtils = LogUtils.build();

    public ConsumerHandler() throws Exception {
        ConfigUtils configUtils = ConfigUtils.build();
        Properties config = configUtils.getProperties(String.format("config.properties"));
        recordDataDecoderCharset = Charset.forName(configUtils.getProperty(config, "recordDataDecoder")).newDecoder();
    }

    @Override
    public void handleRecord(Record record) throws Exception {
        //        if (1 == 1) {
        //            String message = "OMG! OMG! GG when processing a stream record!!!";
        //            System.err.println(message);
        //            throw new Exception(message);
        //        }
        String recordData = recordDataDecoderCharset.decode(record.getData()).toString();
        logUtils.info(String.format("Processed recordData=%s, sequenceNumber=%s.", recordData,
                record.getSequenceNumber()));
    }

    @Override
    public void alertConsumerRestart() {
        String message = "OMG! OMG! KinesisConsumer restart!!!";
        System.err.println(message);
        logUtils.error(new Exception(message));
    }
}
