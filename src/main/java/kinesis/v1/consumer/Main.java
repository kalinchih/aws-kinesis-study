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
     *
     * @throws UnknownHostException
     */
    public static void main(String[] args) {
        try {
            // Load application config
            ConfigUtils configUtils = ConfigUtils.build();
            Properties config = configUtils.getProperties("config.properties");
            String accessKeyId = configUtils.getProperty(config, "aws.credential.access_key_id");
            String accessSecretKey = configUtils.getProperty(config, "aws.credential.access_secret_key");
            String awsRegion = configUtils.getProperty(config, "aws.credential.region");
            String streamName = configUtils.getProperty(config, "aws.kinesis.stream_name");
            String consumerName = configUtils.getProperty(config, "aws.kinesis.consumer_name");
            String initialPositionInStream = configUtils.getProperty(config, "aws.kinesis" + ".consumer_initial_position_in_stream");
            int maxPollRecordCount = Integer.parseInt(configUtils.getProperty(config, "aws.kinesis.consumer_max_poll_record_count"));
            long processRetryDelayMillis = Long.parseLong(configUtils.getProperty(config, "aws.kinesis.consumer_process_retry_delay_millis"));
            int checkpointMaxRetryCount = Integer.parseInt(configUtils.getProperty(config, "aws.kinesis.consumer_checkpoint_max_retry_count"));
            long checkpointRetryDelayMillis = Long.parseLong(configUtils.getProperty(config, "aws.kinesis.consumer_checkpoint_retry_delay_millis"));
            String recordDataDecoderCharset = configUtils.getProperty(config, "aws.kinesis.consumer_record_data_decorder_charset");
            // Create KinesisStreamConsumerConfig
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
            AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
            KinesisStreamConsumerConfig kinesisStreamConsumerConfig =
                    new KinesisStreamConsumerConfig(awsStaticCredentialsProvider, awsRegion, streamName, consumerName);
            // Set KinesisStreamConsumerConfig optional config
            kinesisStreamConsumerConfig.setInitialPositionInStream(InitialPositionInStream.valueOf(initialPositionInStream));
            kinesisStreamConsumerConfig.setMaxPollRecordCount(maxPollRecordCount);
            kinesisStreamConsumerConfig.setProcessRetryDelayMillis(processRetryDelayMillis);
            kinesisStreamConsumerConfig.setCheckpointMaxRetryCount(checkpointMaxRetryCount);
            kinesisStreamConsumerConfig.setCheckpointRetryDelayMillis(checkpointRetryDelayMillis);
            kinesisStreamConsumerConfig.setRecordDataDecorder(Charset.forName(recordDataDecoderCharset).newDecoder());
            // Create and start KinesisStreamConsumer
            KinesisStreamConsumer kinesisStreamConsumer = new KinesisStreamConsumer(kinesisStreamConsumerConfig);
            kinesisStreamConsumer.start();
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
