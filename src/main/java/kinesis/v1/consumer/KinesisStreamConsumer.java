package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

import java.net.InetAddress;
import java.util.UUID;

public final class KinesisStreamConsumer {

    private KinesisStreamConsumerConfig config;

    public KinesisStreamConsumer(KinesisStreamConsumerConfig kinesisStreamConsumerConfig) throws Exception {
        this.config = kinesisStreamConsumerConfig;
    }

    public void start() throws KinesisStreamConsumerInitException {
        try {
            String workerId = String.format("%s-%s", InetAddress.getLocalHost().getCanonicalHostName(),
                    UUID.randomUUID());
            KinesisClientLibConfiguration kclConfiguration =
                    new KinesisClientLibConfiguration(config.getConsumerName(), config.getStreamName(),
                            config.getAwsCredentialsProvider(), workerId);
            kclConfiguration.withRegionName(config.getAwsRegion());
            kclConfiguration.withInitialPositionInStream(config.getInitialPositionInStream());
            kclConfiguration.withMaxRecords(config.getMaxPollRecordCount());
            IRecordProcessorFactory recordProcessorFactory =
                    new KinesisStreamRecordProcessorFactory(config.getProcessRetryDelayMillis(),
                            config.getCheckpointMaxRetryCount(), config.getCheckpointRetryDelayMillis(),
                            config.getRecordDataDecorder());
            IMetricsFactory metricsFactory = new NullMetricsFactory();
            Worker worker =
                    new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
            worker.run();
        } catch (Exception e) {
            throw new KinesisStreamConsumerInitException(e);
        }
    }
}