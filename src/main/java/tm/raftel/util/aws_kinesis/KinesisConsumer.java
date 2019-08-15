package tm.raftel.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import tm.raftel.util.date.DateUtils;
import tm.raftel.util.log.LogUtils;
import tm.raftel.util.random.RandomUtils;

import java.net.InetAddress;
import java.time.Instant;

public final class KinesisConsumer {

    private final LogUtils logUtils = LogUtils.build();
    private KinesisConsumerBag bag;
    private KinesisConsumerWorkerWithId workerWithId;

    public KinesisConsumer(KinesisConsumerBag bag) {
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
        this.bag.setKinesisConsumer(this);
        this.workerWithId.getWorker().run();
    }

    private KinesisConsumerWorkerWithId createWorker() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            ip = "";
        }
        Instant instant = Instant.now();
        String workerId = String.format("ConsumerWorker@%s-Since%s#%s", ip, DateUtils.getEpochMs(), RandomUtils.getRandom(100));
        KinesisClientLibConfiguration kclConfiguration =
                new KinesisClientLibConfiguration(bag.getConsumerName(), bag.getStreamName(), bag.getAwsCredentialsProvider(), workerId);
        kclConfiguration.withRegionName(bag.getAwsRegion());
        kclConfiguration.withInitialPositionInStream(bag.getInitialPositionInStream());
        kclConfiguration.withMaxRecords(bag.getMaxPollRecordCount());
        kclConfiguration.withInitialLeaseTableReadCapacity(bag.getInitialLeaseTableReadCapacity());
        kclConfiguration.withInitialLeaseTableWriteCapacity(bag.getInitialLeaseTableWriteCapacity());
        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(bag);
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        return new KinesisConsumerWorkerWithId(worker, workerId);
    }

    void restart(String shardId, ShutdownReason reason) {
        KinesisConsumerWorkerWithId oldKinesisConsumerWorkerWithId = this.workerWithId;
        // log
        KinesisConsumerLog kinesisConsumerLog = new KinesisConsumerLog(bag, shardId);
        KinesisConsumerRestartException restartConsumerExpception =
                new KinesisConsumerRestartException(oldKinesisConsumerWorkerWithId.getWorkerId(), bag.getProcessRetryDelayMillis(), reason,
                        kinesisConsumerLog);
        logUtils.warn(restartConsumerExpception);
        // TODO: alert
        try {
            Thread.sleep(bag.getProcessRetryDelayMillis());
        } catch (InterruptedException interruptedException) {
            // ignore
        }
        oldKinesisConsumerWorkerWithId.getWorker().shutdown();
        start();
    }
}

