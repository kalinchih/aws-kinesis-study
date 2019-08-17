import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import k0.util.config.ConfigUtils;
import k0.util.phase.PhaseUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {

    private String phase;
    private String accessKeyId;
    private String accessSecretKey;
    private String region;
    private String kinesisStreamName;

    public static void main(String[] args) {
        try {
            Producer producer = new Producer();
            producer.setupPhase(args);
            producer.loadConfig();
            int putRequestCount = 10;
            int recordsInOnePutRequest = 1;
            producer.produce(putRequestCount, recordsInOnePutRequest, 1000);
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void setupPhase(String[] args) throws AppFatalException {
        if (args.length > 0) {
            phase = StringUtils.trim(args[0]);
            PhaseUtils.build().setPhase(phase);
        } else {
            throw new AppFatalException("Cannot determine phase");
        }
    }

    private void loadConfig() throws Exception {
        ConfigUtils configUtils = ConfigUtils.build();
        Properties config = configUtils.getProperties("config.properties");
        accessKeyId = configUtils.getProperty(config, "streamAccessKeyId");
        accessSecretKey = configUtils.getProperty(config, "streamAccessSecretKey");
        region = configUtils.getProperty(config, "streamRegion");
        kinesisStreamName = configUtils.getProperty(config, "streamName");
    }

    private void produce(int putRequestCount, int recordsInOnePutRequest, int delayMillis) throws Exception {
        // Set AWS credentials
        AWSCredentials creds = new BasicAWSCredentials(accessKeyId, accessSecretKey);
        AmazonKinesis kinesisClient =
                AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(region).build();
        final ObjectMapper mapper = new ObjectMapper();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(kinesisStreamName);
        for (int i = 0; i < putRequestCount; i++) {
            List<RecordObject> recordObjects = createRecordObjects(recordsInOnePutRequest);
            List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
            for (int j = 0; j < recordsInOnePutRequest; j++) {
                RecordObject recordObject = recordObjects.get(j);
                recordObject.incrementRecordCount();
                recordObject.setTimestampToNow();
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(mapper.writeValueAsString(recordObject).getBytes()));
                putRecordsRequestEntry.setPartitionKey(recordObject.partitionKey);
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
            }
            putRecordsRequest.setRecords(putRecordsRequestEntryList);
            PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
            System.out.println("Put result : " + putRecordsResult);
            Thread.sleep(delayMillis);
        }
    }

    private List<RecordObject> createRecordObjects(int recordsInOnePutRequest) {
        List<RecordObject> recordObjects = new ArrayList<>();
        for (int i = 0; i < recordsInOnePutRequest; i++) {
            RecordObject recordObject = new RecordObject(String.valueOf(i));
            recordObjects.add(recordObject);
        }
        return recordObjects;
    }
}