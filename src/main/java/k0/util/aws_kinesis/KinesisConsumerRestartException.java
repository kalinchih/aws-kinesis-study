package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

public class KinesisConsumerRestartException extends Exception {

    private static final long serialVersionUID = 1L;

    public KinesisConsumerRestartException(String shutdownWorkerId, long processRetryDelayMillis,
            ShutdownReason reason, KinesisLog kinesisLog) {
        super(String.format("Restart %s after %s ms. ShutdownWorkerId=%s. ShutdownReason=%s. KinesisConsumerLog=%s.",
                KinesisConsumer.class.getName(), shutdownWorkerId, processRetryDelayMillis, reason.name(), kinesisLog));
    }
}
