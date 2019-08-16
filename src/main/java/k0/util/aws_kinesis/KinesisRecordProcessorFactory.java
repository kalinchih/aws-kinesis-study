package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private KinesisConsumerConfig bag;
    private KinesisConsumerHandler recordHandler;

    KinesisRecordProcessorFactory(KinesisConsumerConfig bag, KinesisConsumerHandler recordHandler) {
        this.bag = bag;
        this.recordHandler = recordHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(bag, recordHandler);
    }
}