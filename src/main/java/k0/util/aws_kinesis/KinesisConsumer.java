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

    private KinesisConsumerConfig config;
    private KinesisConsumerHelper consumerHelper;
    private KinesisLogger logger;
    private KinesisConsumerWorkerWithId workerWithId;

    public KinesisConsumer(KinesisConsumerConfig config, KinesisConsumerHelper consumerHelper) {
        this.config = config;
        this.consumerHelper = consumerHelper;
        this.logger = KinesisLogger.build();
    }

    Worker getWorker() {
        return workerWithId.getWorker();
    }

    public String getWorkerId() {
        return workerWithId.getWorkerId();
    }

    public void subscribeStream() {
        this.workerWithId = createWorker();
        // Before worker run(), put self in config to for restart()
        this.config.setKinesisConsumer(this);
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
        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(config.getConsumerName(),
                config.getStreamName(), config.getAwsCredentialsProvider(), workerId);
        kclConfiguration.withRegionName(config.getAwsRegion());
        kclConfiguration.withInitialPositionInStream(config.getInitialPositionInStream());
        kclConfiguration.withMaxRecords(config.getMaxPollRecordCount());
        kclConfiguration.withInitialLeaseTableReadCapacity(config.getInitialLeaseTableReadCapacity());
        kclConfiguration.withInitialLeaseTableWriteCapacity(config.getInitialLeaseTableWriteCapacity());
        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(config, consumerHelper);
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker =
                new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration).metricsFactory(metricsFactory).build();
        return new KinesisConsumerWorkerWithId(worker, workerId);
    }

    void restart(String shardId, ShutdownReason reason) {
        KinesisConsumerWorkerWithId oldKinesisConsumerWorkerWithId = this.workerWithId;
        // log
        KinesisLog kinesisLog = new KinesisLog(config, shardId);
        KinesisConsumerRestartException restartConsumerExpception =
                new KinesisConsumerRestartException(oldKinesisConsumerWorkerWithId.getWorkerId(),
                        config.getConsumerRestartDelayMillis(), reason, kinesisLog);
        logger.warn(restartConsumerExpception);
        consumerHelper.alertConsumerRestart();
        try {
            Thread.sleep(config.getConsumerRestartDelayMillis());
        } catch (InterruptedException interruptedException) {
            // ignore
        }
        oldKinesisConsumerWorkerWithId.getWorker().shutdown();
        subscribeStream();
    }
}

