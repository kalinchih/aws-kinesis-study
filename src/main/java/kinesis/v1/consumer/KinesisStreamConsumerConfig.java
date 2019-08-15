package kinesis.v1.consumer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class KinesisStreamConsumerConfig {

    private AWSCredentialsProvider awsCredentialsProvider;
    private String awsRegion;
    private String streamName;
    private String consumerName;
    private int maxPollRecordCount = 100;
    private long processRetryDelayMillis = 60000;
    private int checkpointMaxRetryCount = 10;
    private long checkpointRetryDelayMillis = 1000;
    private CharsetDecoder recordDataDecorder = Charset.forName("UTF-8").newDecoder();
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;

    public KinesisStreamConsumerConfig(AWSCredentialsProvider awsCredentialsProvider, String awsRegion,
            String streamName, String consumerName) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.awsRegion = awsRegion;
        this.streamName = streamName;
        this.consumerName = consumerName;
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return awsCredentialsProvider;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    /**
     * Default value: 100
     *
     * @return
     */
    public int getMaxPollRecordCount() {
        return maxPollRecordCount;
    }

    public void setMaxPollRecordCount(int maxPollRecordCount) {
        this.maxPollRecordCount = maxPollRecordCount;
    }

    /**
     * Default value: 60000
     *
     * @return
     */
    public long getProcessRetryDelayMillis() {
        return processRetryDelayMillis;
    }

    public void setProcessRetryDelayMillis(long processRetryDelayMillis) {
        this.processRetryDelayMillis = processRetryDelayMillis;
    }

    /**
     * Default value: 10
     *
     * @return
     */
    public int getCheckpointMaxRetryCount() {
        return checkpointMaxRetryCount;
    }

    public void setCheckpointMaxRetryCount(int checkpointMaxRetryCount) {
        this.checkpointMaxRetryCount = checkpointMaxRetryCount;
    }

    /**
     * Default value: 1000
     *
     * @return
     */
    public long getCheckpointRetryDelayMillis() {
        return checkpointRetryDelayMillis;
    }

    public void setCheckpointRetryDelayMillis(long checkpointRetryDelayMillis) {
        this.checkpointRetryDelayMillis = checkpointRetryDelayMillis;
    }

    /**
     * Default value: Charset.forName("UTF-8").newDecoder()
     *
     * @return
     */
    public CharsetDecoder getRecordDataDecorder() {
        return recordDataDecorder;
    }

    public void setRecordDataDecorder(CharsetDecoder recordDataDecorder) {
        this.recordDataDecorder = recordDataDecorder;
    }

    /**
     * Default value: InitialPositionInStream.TRIM_HORIZON
     *
     * @return
     */
    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    public void setInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        this.initialPositionInStream = initialPositionInStream;
    }
}
