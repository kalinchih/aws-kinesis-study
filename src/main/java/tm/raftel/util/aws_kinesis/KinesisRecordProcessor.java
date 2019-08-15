package tm.raftel.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tm.raftel.util.log.LogUtils;

import java.util.List;

public class KinesisRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(KinesisConsumer.class.getPackage().getName());
    private final LogUtils logUtils = LogUtils.build();
    private KinesisConsumerBag bag;
    private String shardId;

    KinesisRecordProcessor(KinesisConsumerBag bag) {
        this.bag = bag;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        logUtils.info(String.format("Initialized %s. KinesisConsumerLog=%s.", this.toString(), new KinesisConsumerLog(bag, shardId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        String consumerWorkerId = bag.getKinesisConsumer().getWorkerId();
        LOG.info(String.format("Start to process %s records. KinesisConsumerLog=%s.", records.size(),
                new KinesisConsumerLog(bag, shardId, consumerWorkerId)));
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
                    new KinesisProcessRecordException(new KinesisConsumerLog(bag, shardId, consumerWorkerId, ongoingSequenceNumber), e);
            logUtils.error(processRecordException);
            if (completedSequenceNumber != null) {
                checkpoint(checkpointer, completedSequenceNumber);
            }
            shutdown(checkpointer, ShutdownReason.REQUESTED);
        }
    }

    private void processSingleRecord(Record record, int indexInRecords) throws Exception {
        String recordData = bag.getRecordDataDecoder().decode(record.getData()).toString();
        LOG.info(String.format("Start to process record. KinesisConsumerLog=%s.",
                new KinesisConsumerLog(bag, shardId, bag.getKinesisConsumer().getWorkerId(), record.getSequenceNumber())));
        // TODO: biz logic here
        //
        //        if (indexInRecords > 1) {
        //            throw new Exception("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG");
        //        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        String consumerWorkerId = bag.getKinesisConsumer().getWorkerId();
        for (int i = 0; i < bag.getCheckpointMaxRetryCount(); i++) {
            try {
                logUtils.info(String.format("Start to cehckpoint. KinesisConsumerLog=%s.",
                        new KinesisConsumerLog(bag, shardId, bag.getKinesisConsumer().getWorkerId(), sequenceNumber)));
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
                if (i >= (bag.getCheckpointMaxRetryCount() - 1)) {
                    String message = String.format("Upon getCheckpointMaxRetryCount after %s attempts.", bag.getCheckpointMaxRetryCount());
                    KinesisConsumerCheckpointException checkpointException =
                            new KinesisConsumerCheckpointException(message, new KinesisConsumerLog(bag, shardId, consumerWorkerId, sequenceNumber),
                                    e);
                    logUtils.error(checkpointException);
                    break;
                } else {
                    String message = String.format("Retry checkpoint in attempts: %/%s.", (i + 1), bag.getCheckpointMaxRetryCount());
                    KinesisConsumerCheckpointException checkpointException =
                            new KinesisConsumerCheckpointException(message, new KinesisConsumerLog(bag, shardId, consumerWorkerId, sequenceNumber),
                                    e);
                    logUtils.warn(checkpointException);
                }
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
        bag.getKinesisConsumer().restart(shardId, reason);
    }
}