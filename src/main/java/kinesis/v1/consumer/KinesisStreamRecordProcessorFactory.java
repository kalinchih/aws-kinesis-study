package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KinesisStreamRecordProcessorFactory implements IRecordProcessorFactory {

    private KinesisStreamConsumerBag bag;

    KinesisStreamRecordProcessorFactory(KinesisStreamConsumerBag bag) {
        this.bag = bag;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisStreamRecordProcessor(bag);
    }
}