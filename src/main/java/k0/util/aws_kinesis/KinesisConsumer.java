package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import k0.util.date.DateUtils;
import k0.util.random.RandomUtils;

import java.net.InetAddress;
import java.time.Instant;

public final class KinesisConsumer {

    private KinesisConsumerConfig bag;
    private KinesisConsumerHandler recordHandler;
    private KinesisLogger logger;
    private KinesisConsumerWorkerWithId workerWithId;

    public KinesisConsumer(KinesisConsumerConfig config, KinesisConsumerHandler recordHandler) {
        this.bag = config;
        this.recordHandler = recordHandler;
        this.logger = KinesisLogger.build(config.isEnableInfoLog());
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
        String workerId = String.format("ConsumerWorker@%s-Since%s#%s", ip, DateUtils.getEpochMs(),
                RandomUtils.getRandom(100));
        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(bag.getConsumerName(),
                bag.getStreamName(), bag.getAwsCredentialsProvider(), workerId);
        kclConfiguration.withRegionName(bag.getAwsRegion());
        kclConfiguration.withInitialPositionInStream(bag.getInitialPositionInStream());
        kclConfiguration.withMaxRecords(bag.getMaxPollRecordCount());
        kclConfiguration.withInitialLeaseTableReadCapacity(bag.getInitialLeaseTableReadCapacity());
        kclConfiguration.withInitialLeaseTableWriteCapacity(bag.getInitialLeaseTableWriteCapacity());
        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(bag, recordHandler);
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        return new KinesisConsumerWorkerWithId(worker, workerId);
    }

    void restart(String shardId, ShutdownReason reason) {
        KinesisConsumerWorkerWithId oldKinesisConsumerWorkerWithId = this.workerWithId;
        // log
        KinesisLog kinesisLog = new KinesisLog(bag, shardId);
        KinesisConsumerRestartException restartConsumerExpception =
                new KinesisConsumerRestartException(oldKinesisConsumerWorkerWithId.getWorkerId(),
                        bag.getConsumerRestartDelayMillis(), reason, kinesisLog);
        logger.warn(restartConsumerExpception);
        recordHandler.alertConsumerRestart();
        try {
            Thread.sleep(bag.getConsumerRestartDelayMillis());
        } catch (InterruptedException interruptedException) {
            // ignore
        }
        oldKinesisConsumerWorkerWithId.getWorker().shutdown();
        start();
    }
}

