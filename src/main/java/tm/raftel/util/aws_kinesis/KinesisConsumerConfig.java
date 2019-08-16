package tm.raftel.util.aws_kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class KinesisConsumerConfig {

    private AWSCredentialsProvider awsCredentialsProvider;
    private String awsRegion;
    private String streamName;
    private String consumerName;
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
    private int initialLeaseTableReadCapacity = 1;
    private int initialLeaseTableWriteCapacity = 1;
    private int maxPollRecordCount = 100;
    private long consumerRestartDelayMillis = 60000;
    private int checkpointMaxRetryCount = 10;
    private long checkpointRetryDelayMillis = 30000;
    private CharsetDecoder recordDataDecoder = Charset.forName("UTF8").newDecoder();
    private boolean enableInfoLog = false;
    private KinesisConsumer kinesisConsumer;

    public KinesisConsumerConfig(AWSCredentialsProvider awsCredentialsProvider, String awsRegion, String streamName,
            String consumerName) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.awsRegion = awsRegion;
        this.streamName = streamName;
        this.consumerName = consumerName;
    }

    /**
     * Default value: 1
     *
     * @return
     */
    public int getInitialLeaseTableReadCapacity() {
        return initialLeaseTableReadCapacity;
    }

    public void setInitialLeaseTableReadCapacity(int initialLeaseTableReadCapacity) {
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
    }

    /**
     * Default value: 1
     *
     * @return
     */
    public int getInitialLeaseTableWriteCapacity() {
        return initialLeaseTableWriteCapacity;
    }

    public void setInitialLeaseTableWriteCapacity(int initialLeaseTableWriteCapacity) {
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
    }

    public KinesisConsumer getKinesisConsumer() {
        return kinesisConsumer;
    }

    void setKinesisConsumer(KinesisConsumer kinesisConsumer) {
        this.kinesisConsumer = kinesisConsumer;
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
    public long getConsumerRestartDelayMillis() {
        return consumerRestartDelayMillis;
    }

    public void setConsumerRestartDelayMillis(long consumerRestartDelayMillis) {
        this.consumerRestartDelayMillis = consumerRestartDelayMillis;
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
     * Default value: 30000
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
     * Default value: Charset.forName("UTF8").newDecoder()
     *
     * @return
     */
    public CharsetDecoder getRecordDataDecoder() {
        return recordDataDecoder;
    }

    public void setRecordDataDecoder(CharsetDecoder recordDataDecoder) {
        this.recordDataDecoder = recordDataDecoder;
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

    /**
     * Default value: false
     *
     * @return
     */
    public boolean isEnableInfoLog() {
        return enableInfoLog;
    }

    public void setEnableInfoLog(boolean enableInfoLog) {
        this.enableInfoLog = enableInfoLog;
    }
}
