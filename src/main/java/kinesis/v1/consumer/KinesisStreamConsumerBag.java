package kinesis.v1.consumer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class KinesisStreamConsumerBag {

    private AWSCredentialsProvider awsCredentialsProvider;
    private String awsRegion;
    private String streamName;
    private String consumerName;
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
    private int initialLeaseTableReadCapacity = 1;
    private int initialLeaseTableWriteCapacity = 1;
    private int maxPollRecordCount = 100;
    private long processRetryDelayMillis = 60000;
    private int checkpointMaxRetryCount = 10;
    private long checkpointRetryDelayMillis = 30000;
    private CharsetDecoder recordDataDecoder = Charset.forName("UTF8").newDecoder();
    private KinesisStreamConsumer kinesisStreamConsumer;

    public KinesisStreamConsumerBag(AWSCredentialsProvider awsCredentialsProvider, String awsRegion,
            String streamName, String consumerName) {
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

    public KinesisStreamConsumer getKinesisStreamConsumer() {
        return kinesisStreamConsumer;
    }

    void setKinesisStreamConsumer(KinesisStreamConsumer kinesisStreamConsumer) {
        this.kinesisStreamConsumer = kinesisStreamConsumer;
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
}