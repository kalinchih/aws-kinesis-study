package kinesis.v1.consumer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import utils.config.ConfigUtils;

import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Properties;

public class Main {

    public Main() throws Exception {
    }

    /**
     * Consumer method to start workers
     *
     * @param args
     * @throws UnknownHostException
     */
    public static void main(String[] args) {
        try {
            // Load settings from config file
            ConfigUtils configUtils = ConfigUtils.build();
            Properties config = configUtils.getProperties("config.properties");
            // Create bag
            String accessKeyId = configUtils.getProperty(config, "accessKeyId");
            String accessSecretKey = configUtils.getProperty(config, "accessSecretKey");
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
            AWSStaticCredentialsProvider awsStaticCredentialsProvider =
                    new AWSStaticCredentialsProvider(awsCredentials);
            String streamRegion = configUtils.getProperty(config, "streamRegion");
            String streamName = configUtils.getProperty(config, "streamName");
            String consumerName = configUtils.getProperty(config, "consumerName");
            KinesisStreamConsumerBag bag = new KinesisStreamConsumerBag(awsStaticCredentialsProvider, streamRegion,
                    streamName, consumerName);
            // Set bag optional settings
            String initialPositionInStream = configUtils.getProperty(config, "initialPositionInStream");
            bag.setInitialPositionInStream(InitialPositionInStream.valueOf(initialPositionInStream));
            int initialLeaseTableReadCapacity = Integer.parseInt(configUtils.getProperty(config,
                    "initialLeaseTableReadCapacity"));
            bag.setInitialLeaseTableReadCapacity(initialLeaseTableReadCapacity);
            int initialLeaseTableWriteCapacity = Integer.parseInt(configUtils.getProperty(config,
                    "initialLeaseTableWriteCapacity"));
            bag.setInitialLeaseTableWriteCapacity(initialLeaseTableWriteCapacity);
            int maxPollRecordCount = Integer.parseInt(configUtils.getProperty(config, "maxPollRecordCount"));
            bag.setMaxPollRecordCount(maxPollRecordCount);
            long processRetryDelayMillis = Long.parseLong(configUtils.getProperty(config, "processRetryDelayMillis"));
            bag.setProcessRetryDelayMillis(processRetryDelayMillis);
            int checkpointMaxRetryCount = Integer.parseInt(configUtils.getProperty(config, "checkpointMaxRetryCount"));
            bag.setCheckpointMaxRetryCount(checkpointMaxRetryCount);
            long checkpointRetryDelayMillis = Long.parseLong(configUtils.getProperty(config,
                    "checkpointRetryDelayMillis"));
            bag.setCheckpointRetryDelayMillis(checkpointRetryDelayMillis);
            String recordDataDecoderCharset = configUtils.getProperty(config, "recordDataDecoder");
            bag.setRecordDataDecoder(Charset.forName(recordDataDecoderCharset).newDecoder());
            // Create and start KinesisStreamConsumer
            KinesisStreamConsumer kinesisStreamConsumer = new KinesisStreamConsumer(bag);
            kinesisStreamConsumer.start();
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
