package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class KinesisStreamRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(KinesisStreamRecordProcessor.class);
    private KinesisStreamConsumerBag bag;
    private String shardId;

    KinesisStreamRecordProcessor(KinesisStreamConsumerBag bag) {
        this.bag = bag;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        LOG.info(String.format("Initialized %s for shard: %s, config: %s.", this.getClass().getSimpleName(), shardId,
                bag.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        String consumerWorkerId = bag.getKinesisStreamConsumer().getWorkerId();
        LOG.info(String.format("%s processing %s records from shard: %s.", consumerWorkerId, records.size(), shardId));
        String ongoingSequenceNumber = null;
        String completedSequenceNumber = null;
        try {
            for (Record record : records) {
                ongoingSequenceNumber = record.getSequenceNumber();
                processSingleRecord(record);
                completedSequenceNumber = record.getSequenceNumber();
            }
            checkpoint(checkpointer, completedSequenceNumber);
        } catch (Exception e) {
            LOG.error(String.format("%s encounter exception when processing record with sequenceNumber: %s, shard: " + "%s" + ".", consumerWorkerId, ongoingSequenceNumber, shardId));
            if (completedSequenceNumber != null) {
                checkpoint(checkpointer, completedSequenceNumber);
            }
            try {
                Thread.sleep(bag.getProcessRetryDelayMillis());
            } catch (InterruptedException interruptedException) {
                // ignore
            }
        }
    }

    /**
     * @param record
     * @throws Exception
     */
    private void processSingleRecord(Record record) throws Exception {
        // TODO: biz logic here
        String recordData = bag.getRecordDataDecoder().decode(record.getData()).toString();
        System.out.println(String.format("Processed %s with sequenceNumber: %s.", recordData,
                record.getSequenceNumber()));
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        String consumerWorkerId = bag.getKinesisStreamConsumer().getWorkerId();
        for (int i = 0; i < bag.getCheckpointMaxRetryCount(); i++) {
            try {
                LOG.info(String.format("%s checkpoint shard: %s, sequenceNumber: %s.", consumerWorkerId, shardId,
                        checkpointer.prepareCheckpoint().getPendingCheckpoint().getSequenceNumber()));
                if (StringUtils.isNotBlank(sequenceNumber)) {
                    checkpointer.checkpoint(sequenceNumber);
                } else {
                    checkpointer.checkpoint();
                }
                break;
            } catch (ShutdownException shutdownException) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info(String.format("%s caught shutdown exception, skipping checkpoint. shard: %s.",
                        consumerWorkerId, shardId), shutdownException);
                break;
            } catch (ThrottlingException throttlingException) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (bag.getCheckpointMaxRetryCount() - 1)) {
                    LOG.error(String.format("%s checkpoint failed after %s attempts. shard: %s.", consumerWorkerId,
                            (i + 1), shardId), throttlingException);
                    break;
                } else {
                    LOG.warn(String.format("%s checkpoint failed. attempts: %/%s, shard: %s.", consumerWorkerId,
                            (i + 1), bag.getCheckpointMaxRetryCount(), shardId), throttlingException);
                }
            } catch (InvalidStateException invalidStateException) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error(String.format("%s cannot save checkpoint to the DynamoDB table used by the KCL. shard: %s."
                        , consumerWorkerId, shardId), invalidStateException);
                break;
            }
            try {
                Thread.sleep(bag.getCheckpointRetryDelayMillis());
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
        String consumerWorkerId = bag.getKinesisStreamConsumer().getWorkerId();
        LOG.warn(String.format("Shutting down %s for shard: %s. Another %s will be re-created and started after %s " + "milliseconds.", consumerWorkerId, shardId, KinesisStreamConsumer.class.getSimpleName(), bag.getProcessRetryDelayMillis()));
        bag.getKinesisStreamConsumer().shutdown();
        try {
            Thread.sleep(bag.getProcessRetryDelayMillis());
        } catch (InterruptedException interruptedException) {
            // ignore
        }
        LOG.warn(String.format("Restarting %s for shard: %s.", KinesisStreamConsumer.class.getSimpleName(), shardId));
        // Create and start another KinesisStreamConsumer
        KinesisStreamConsumer kinesisStreamConsumer = new KinesisStreamConsumer(bag);
        kinesisStreamConsumer.start();
    }
}