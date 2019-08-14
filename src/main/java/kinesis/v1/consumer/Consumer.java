package kinesis.v1.consumer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import utils.config.ConfigUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

/**
 * Sample consumer application for Amazon Kinesis Streams to display records.
 */
public final class Consumer {

    // https://docs.aws.amazon.com/en_us/streams/latest/dev/kinesis-record-processor-additional-considerations.html
    // https://docs.aws.amazon.com/en_us/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax
    private String accessKeyId;
    private String accessSecretKey;
    private String region;
    private String kinesisStreamName;
    private String kinesisConsumerName;
    private String kinesisConsumerInitialPositionInStream;

    private Consumer() throws Exception {
        ConfigUtils configUtils = ConfigUtils.build();
        Properties config = configUtils.getProperties("config.properties");
        accessKeyId = configUtils.getProperty(config, "aws.credential.access_key_id");
        accessSecretKey = configUtils.getProperty(config, "aws.credential.access_secret_key");
        region = configUtils.getProperty(config, "aws.credential.region");
        kinesisStreamName = configUtils.getProperty(config, "aws.kinesis.stream_name");
        kinesisConsumerName = configUtils.getProperty(config, "aws.kinesis.consumer_name");
        kinesisConsumerInitialPositionInStream = configUtils.getProperty(config, "aws.kinesis.consumer_initial_position_in_stream");
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
            Consumer consumer = new Consumer();
            consumer.run();
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void run() throws Exception {
        // Set AWS credentials
        AWSCredentials creds = new BasicAWSCredentials(accessKeyId, accessSecretKey);
        // Set KCL configuration
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kclConfiguration =
                new KinesisClientLibConfiguration(kinesisConsumerName, kinesisStreamName, new AWSStaticCredentialsProvider(creds), workerId);
        kclConfiguration.withRegionName(region);
        kclConfiguration.withInitialPositionInStream(InitialPositionInStream.valueOf(kinesisConsumerInitialPositionInStream));
        kclConfiguration.withMaxRecords(10);
        // Start workers
        IRecordProcessorFactory recordProcessorFactory = new KinesisConsumerFactory();
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        System.out.printf("Running %s to process stream %s as worker %s...\n", kinesisConsumerName, kinesisStreamName, workerId);
        worker.run();
    }
}