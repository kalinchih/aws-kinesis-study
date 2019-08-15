package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

import java.nio.charset.CharsetDecoder;

public class KinesisStreamRecordProcessorFactory implements IRecordProcessorFactory {

    private long processRetryDelayMillis;
    private int checkpointMaxRetryCount;
    private long checkpointRetryDelayMillis;
    private CharsetDecoder recordDataDecorder;

    KinesisStreamRecordProcessorFactory(long processRetryDelayMillis, int checkpointMaxRetryCount,
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
    public IRecordProcessor createProcessor() {
        return new KinesisStreamRecordProcessor(processRetryDelayMillis, checkpointMaxRetryCount,
                checkpointRetryDelayMillis, recordDataDecorder);
    }
}