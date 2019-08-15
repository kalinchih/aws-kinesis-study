package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Random;

public final class KinesisStreamConsumer {

    private static final Log LOG = LogFactory.getLog(KinesisStreamConsumer.class);
    private KinesisStreamConsumerBag bag;
    private KinesisStreamConsumerWorkerWithId workerWithId;

    public KinesisStreamConsumer(KinesisStreamConsumerBag bag) {
        this.bag = bag;
    }

    Worker getWorker() {
        return workerWithId.getWorker();
    }

    public String getWorkerId() {
        return workerWithId.getWorkerId();
    }

    public void start() {
        this.workerWithId = createWorker();
        // Before worker run(), put self in bag to for restart()
        this.bag.setKinesisStreamConsumer(this);
        this.workerWithId.getWorker().run();
    }

    private KinesisStreamConsumerWorkerWithId createWorker() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            ip = "";
        }
        Instant instant = Instant.now();
        String workerId = String.format("ConsumerWorker@%s-Since%s#%s", ip, instant.toEpochMilli(),
                new Random().nextInt(100));
        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(bag.getConsumerName(),
                bag.getStreamName(), bag.getAwsCredentialsProvider(), workerId);
        kclConfiguration.withRegionName(bag.getAwsRegion());
        kclConfiguration.withInitialPositionInStream(bag.getInitialPositionInStream());
        kclConfiguration.withMaxRecords(bag.getMaxPollRecordCount());
        kclConfiguration.withInitialLeaseTableReadCapacity(bag.getInitialLeaseTableReadCapacity());
        kclConfiguration.withInitialLeaseTableWriteCapacity(bag.getInitialLeaseTableWriteCapacity());
        IRecordProcessorFactory recordProcessorFactory = new KinesisStreamRecordProcessorFactory(bag);
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        return new KinesisStreamConsumerWorkerWithId(worker, workerId);
    }

    void restart(String shardId, ShutdownReason reason) {
        KinesisStreamConsumerWorkerWithId oldKinesisStreamConsumerWorkerWithId = this.workerWithId;
        LOG.warn(String.format("Shutting down %s for shard: %s after %s milliseconds. Reason: %s.",
                oldKinesisStreamConsumerWorkerWithId.getWorkerId(), shardId, bag.getProcessRetryDelayMillis(),
                reason.name()));
        // TODO: alert
        try {
            Thread.sleep(bag.getProcessRetryDelayMillis());
        } catch (InterruptedException interruptedException) {
            // ignore
        }
        LOG.warn(String.format("Restarting %s for shard: %s.", KinesisStreamConsumer.class.getSimpleName(), shardId));
        oldKinesisStreamConsumerWorkerWithId.getWorker().shutdown();
        start();
    }
}

