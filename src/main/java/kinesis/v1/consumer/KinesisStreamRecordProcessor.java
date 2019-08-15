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

import java.nio.charset.CharsetDecoder;
import java.util.List;

public class KinesisStreamRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(KinesisStreamRecordProcessor.class);
    private long processRetryDelayMillis;
    private int checkpointMaxRetryCount;
    private long checkpointRetryDelayMillis;
    private CharsetDecoder recordDataDecorder;
    private String shardId;
    private String recordProcessorName;

    KinesisStreamRecordProcessor(long processRetryDelayMillis, int checkpointMaxRetryCount,
            long checkpointRetryDelayMillis, CharsetDecoder recordDataDecorder) {
        this.processRetryDelayMillis = processRetryDelayMillis;
        this.checkpointMaxRetryCount = checkpointMaxRetryCount;
        this.checkpointRetryDelayMillis = checkpointRetryDelayMillis;
        this.recordDataDecorder = recordDataDecorder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        this.recordProcessorName = this.getClass().getSimpleName();
        LOG.info(String.format("Initialized %s for shard: %s, processRetryDelayMillis: %s, checkpointMaxRetryCount: " + "%s, checkpointRetryDelayMillis: %s.", recordProcessorName, shardId, processRetryDelayMillis, checkpointMaxRetryCount, checkpointRetryDelayMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info(String.format("%s processing %s records from shard: %s.", recordProcessorName, records.size(),
                shardId));
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
            LOG.error(String.format("%s encounter exception when processing record with sequenceNumber: %s, shard: " + "%s" + ".", recordProcessorName, ongoingSequenceNumber, shardId));
            if (completedSequenceNumber != null) {
                checkpoint(checkpointer, completedSequenceNumber);
            }
            try {
                Thread.sleep(processRetryDelayMillis);
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
        String recordData = recordDataDecorder.decode(record.getData()).toString();
        System.out.println(String.format("Processed %s with sequenceNumber: %s.", recordData,
                record.getSequenceNumber()));
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        for (int i = 0; i < checkpointMaxRetryCount; i++) {
            try {
                LOG.info(String.format("%s checkpoint shard: %s, sequenceNumber: %s.", recordProcessorName, shardId,
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
                        recordProcessorName, shardId), shutdownException);
                break;
            } catch (ThrottlingException throttlingException) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (checkpointMaxRetryCount - 1)) {
                    LOG.error(String.format("%s checkpoint failed after %s attempts. shard: %s.", recordProcessorName
                            , (i + 1), shardId), throttlingException);
                    break;
                } else {
                    LOG.warn(String.format("%s checkpoint failed. attempts: %/%s, shard: %s.", recordProcessorName,
                            (i + 1), checkpointMaxRetryCount, shardId), throttlingException);
                }
            } catch (InvalidStateException invalidStateException) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error(String.format("%s cannot save checkpoint to the DynamoDB table used by the KCL. shard: %s."
                        , recordProcessorName, shardId), invalidStateException);
                break;
            }
            try {
                Thread.sleep(checkpointRetryDelayMillis);
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
        LOG.info(String.format("Shutting down %s for shard: %s.", recordProcessorName, shardId));
        // Not to checkpoint! processSingleRecord() should have the ability to handle re-poll/non-checkpoint records.
    }
}