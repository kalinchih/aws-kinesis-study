package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private KinesisConsumerConfig bag;
    private KinesisConsumerHelper consumerHelper;

    KinesisRecordProcessorFactory(KinesisConsumerConfig bag, KinesisConsumerHelper consumerHelper) {
        this.bag = bag;
        this.consumerHelper = consumerHelper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(bag, consumerHelper);
    }
}