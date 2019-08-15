package kinesis.v1.producer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import kinesis.v1.RecordObject;
import utils.config.ConfigUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {

    private String accessKeyId;
    private String accessSecretKey;
    private String region;
    private String kinesisStreamName;

    private Producer() throws Exception {
        ConfigUtils configUtils = ConfigUtils.build();
        Properties config = configUtils.getProperties("config.properties");
        accessKeyId = configUtils.getProperty(config, "accessKeyId");
        accessSecretKey = configUtils.getProperty(config, "accessSecretKey");
        region = configUtils.getProperty(config, "streamRegion");
        kinesisStreamName = configUtils.getProperty(config, "streamName");
    }

    public static void main(String[] args) {
        try {
            Producer producer = new Producer();
            int putRequestCount = 10;
            int recordsInOnePutRequest = 1;
            producer.produce(putRequestCount, recordsInOnePutRequest, 1000);
        } catch (Exception e) {
            System.err.println("Caught throwable while processing data.");
            e.printStackTrace();
            System.exit(1);
        }
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