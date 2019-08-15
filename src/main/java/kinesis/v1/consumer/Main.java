package kinesis.v1.consumer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import utils.config.ConfigUtils;

import java.net.UnknownHostException;
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
            ConfigUtils configUtils = ConfigUtils.build();
            Properties config = configUtils.getProperties("config.properties");
            String accessKeyId = configUtils.getProperty(config, "aws.credential.access_key_id");
            String accessSecretKey = configUtils.getProperty(config, "aws.credential.access_secret_key");
            String awsRegion = configUtils.getProperty(config, "aws.credential.region");
            String kinesisStreamName = configUtils.getProperty(config, "aws.kinesis.stream_name");
            String kinesisConsumerName = configUtils.getProperty(config, "aws.kinesis.consumer_name");
            String kinesisConsumerInitialPositionInStream = configUtils.getProperty(config, "aws.kinesis" +
                    ".consumer_initial_position_in_stream");
            // Load config
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
            AWSStaticCredentialsProvider awsStaticCredentialsProvider =
                    new AWSStaticCredentialsProvider(awsCredentials);
            KinesisStreamConsumerConfig kinesisStreamConsumerConfig =
                    new KinesisStreamConsumerConfig(awsStaticCredentialsProvider, awsRegion, kinesisStreamName,
                            kinesisConsumerName);
            // Start consumer
            KinesisStreamConsumer kinesisStreamConsumer = new KinesisStreamConsumer(kinesisStreamConsumerConfig);
            kinesisStreamConsumer.start();
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
