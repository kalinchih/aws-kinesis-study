package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class KinesisRecordProcessor implements IRecordProcessor {

    private KinesisConsumerConfig config;
    private KinesisConsumerHelper consumerHelper;
    private KinesisLogger logger;
    private String shardId;

    KinesisRecordProcessor(KinesisConsumerConfig config, KinesisConsumerHelper consumerHelper) {
        this.config = config;
        this.consumerHelper = consumerHelper;
        this.logger = KinesisLogger.build(config.isEnableInfoLog());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        logger.info(String.format("Initialized %s. KinesisConsumerLog=%s.", this.toString(), new KinesisLog(config,
                shardId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        String consumerWorkerId = config.getKinesisConsumer().getWorkerId();
        logger.info(String.format("Start to process %s records. KinesisConsumerLog=%s.", records.size(),
                new KinesisLog(config, shardId, consumerWorkerId)));
        String ongoingSequenceNumber = null;
        String completedSequenceNumber = null;
        try {
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                ongoingSequenceNumber = record.getSequenceNumber();
                processSingleRecord(record, i);
                completedSequenceNumber = record.getSequenceNumber();
            }
            checkpoint(checkpointer, completedSequenceNumber);
        } catch (Exception e) {
            KinesisProcessRecordException processRecordException =
                    new KinesisProcessRecordException(new KinesisLog(config, shardId, consumerWorkerId,
                            ongoingSequenceNumber), e);
            logger.error(processRecordException);
            if (completedSequenceNumber != null) {
                checkpoint(checkpointer, completedSequenceNumber);
            }
            shutdown(checkpointer, ShutdownReason.REQUESTED);
        }
    }

    private void processSingleRecord(Record record, int indexInRecords) throws Exception {
        logger.info(String.format("Pass record to (%s)%s. KinesisConsumerLog=%s.",
                KinesisConsumerHelper.class.getSimpleName(), consumerHelper.toString(), new KinesisLog(config,
                        shardId, config.getKinesisConsumer().getWorkerId(), record.getSequenceNumber())));
        consumerHelper.handleRecord(record);
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        String consumerWorkerId = config.getKinesisConsumer().getWorkerId();
        for (int i = 0; i < config.getCheckpointMaxRetryCount(); i++) {
            try {
                logger.info(String.format("Start to cehckpoint. KinesisConsumerLog=%s.", new KinesisLog(config,
                        shardId, config.getKinesisConsumer().getWorkerId(), sequenceNumber)));
                if (StringUtils.isNotBlank(sequenceNumber)) {
                    checkpointer.checkpoint(sequenceNumber);
                } else {
                    checkpointer.checkpoint();
                }
                break;
            } catch (ShutdownException shutdownException) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                break;
            } catch (Exception e) {
                if (i >= (config.getCheckpointMaxRetryCount() - 1)) {
                    String message = String.format("Upon getCheckpointMaxRetryCount after %s attempts.",
                            config.getCheckpointMaxRetryCount());
                    KinesisConsumerCheckpointException checkpointException =
                            new KinesisConsumerCheckpointException(message, new KinesisLog(config, shardId,
                                    consumerWorkerId, sequenceNumber), e);
                    logger.error(checkpointException);
                    break;
                } else {
                    String message = String.format("Retry checkpoint in attempts: %/%s.", (i + 1),
                            config.getCheckpointMaxRetryCount());
                    KinesisConsumerCheckpointException checkpointException =
                            new KinesisConsumerCheckpointException(message, new KinesisLog(config, shardId,
                                    consumerWorkerId, sequenceNumber), e);
                    logger.warn(checkpointException);
                }
            }
            try {
                Thread.sleep(config.getCheckpointRetryDelayMillis());
            } catch (InterruptedException interruptedException) {
                // ignore
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        // Not to checkpoint! processSingleRecord() should have the ability to handle re-poll/non-checkpoint records.
        config.getKinesisConsumer().restart(shardId, reason);
    }
}