package tm.raftel.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private KinesisConsumerBag bag;

    KinesisRecordProcessorFactory(KinesisConsumerBag bag) {
        this.bag = bag;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(bag);
    }
}