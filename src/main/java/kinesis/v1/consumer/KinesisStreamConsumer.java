package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

import java.net.InetAddress;
import java.time.Instant;
import java.util.UUID;

public final class KinesisStreamConsumer {

    private KinesisStreamConsumerBag bag;
    private String workerId;
    private Worker worker;

    public KinesisStreamConsumer(KinesisStreamConsumerBag bag) {
        this.bag = bag;
    }

    Worker getWorker() {
        return worker;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void start() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            ip = "";
        }
        Instant instant = Instant.now();
        workerId = String.format("%s-%s-%s", ip, instant.toEpochMilli(), UUID.randomUUID());
        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(bag.getConsumerName(),
                bag.getStreamName(), bag.getAwsCredentialsProvider(), workerId);
        kclConfiguration.withRegionName(bag.getAwsRegion());
        kclConfiguration.withInitialPositionInStream(bag.getInitialPositionInStream());
        kclConfiguration.withMaxRecords(bag.getMaxPollRecordCount());
        kclConfiguration.withInitialLeaseTableReadCapacity(bag.getInitialLeaseTableReadCapacity());
        kclConfiguration.withInitialLeaseTableWriteCapacity(bag.getInitialLeaseTableWriteCapacity());
        IRecordProcessorFactory recordProcessorFactory = new KinesisStreamRecordProcessorFactory(bag);
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        // Put self in bag to create and start another KinesisStreamConsumer when shutdown
        bag.setKinesisStreamConsumer(this);
        worker.run();
    }

    void shutdown() {
        worker.shutdown();
    }
}