# aws-kinesis-study

This application help you to quickly enable an AWS Kinesis stream consumer.
It leverage AWS KCL (Kinesis Client Library) 1.x V1 interface and encapsulated in package `tm.raftel.util.aws_kinesis`.

KCL helps consumer to automatically record (aka, checkpoint) the `sequence number` of the last consumed stream record. The `sequence number` is saved in a DynamoDB table which automatically created by KCL. The table name is the consumerName which configured in `KinesisConsumerConfig`.


---

## How to Start a Consumer to Subscribe Kinesis Stream

### Demo Classes
- Class RecordObject
  - Stream record wrapper 
- Class ProducerApp
  - Produce stream records 
- Class ConsumerApp:
  - Consume stream records


### Key Classes in Pacage tm.raftel.util.aws_kinesis 
- Class KinesisConsumerConfig
  - The config class 
- Class KinesisConsumer
  - A configurable stream consumer has retry/restart abilities
    - Retry timings:
      - Fail to checkpoint consumer sequence number
    - Restart consumer timings:
      - Fail to consume records
      - When exception thrown in customized KinesisConsumerHandler.handleRecord() 
  - If start multiple KinesisConsumer instances, these consumers are managed by KCL and play as load balancing mode
- Interface KinesisConsumerHandler
  - Interface to customized consumer
  - @Override methods:
    - handleRecord(Record record):
      - Do your biz logic here!
      - If exception thrown in this method, the input record will not be checked point trigger a restart flow
    - alertConsumerRestart():
      - Customize alarm for KinesisConsumer restart event


```
// 1. Create KinesisConsumerConfig
KinesisConsumerConfig consumerConfig = new KinesisConsumerConfig(awsStaticCredentialsProvider,
        streamRegion, streamName, consumerName);

// 2. Set KinesisConsumerConfig optional settings (not necessary)
setOptionalSettings(configUtils, configFile, consumerConfig);

// 3. Create KinesisConsumerHandler to implement
ConsumerHandler consumeRecordHandler = new ConsumerHandler();

// 4. Create/start a KinesisConsumer
KinesisConsumer kinesisConsumer = new KinesisConsumer(consumerConfig, consumeRecordHandler);
kinesisConsumer.start();
``` 

---

## Configuration

Here is the KinesisConsumerConfig settings with default value.

- AWS credential required settings
    - accessKeyId 
    - accessSecretKey
- AWS Kinesis consumer required settings
    - streamRegion 
    - streamName
    - consumerName 
- AWS Kinesis consumer optional settings
    - initialPositionInStream = TRIM_HORIZON
    - maxPollRecordCount = 100
    - initialLeaseTableReadCapacity = 1
    - initialLeaseTableWriteCapacity = 1
    - consumeRetryDelayMillis = 60000
    - checkpointMaxRetryCount = 10
    - checkpointRetryDelayMillis = 30000
    - recordDataDecoder = UTF8
    - enableInfoLog = false

---

## Logging

`tm.raftel.util.aws_kinesis` leverages log4j (v1) to output JSON format info, error, system logs.
- Logs:
  - info logs: 
    - logs which are logged by `tm.raftel.util.aws_kinesis`
    - only contain info level logs
  - error logs: 
    - logs which are logged by `tm.raftel.util.aws_kinesis`
    - contain warn, error and fatal logs
  - system logs:
    - logs which are logged by 3rd party libraries such as KCL
  
The logging configuration file is in `/resources/{phase}/tm.raftel.util.aws_kinesis-log4j.xml`.
You can toggle this library to write info log by KinesisConsumerConfig.setEnableInfoLog()


---

## 3rd Party Libraries

- amazon-kinesis-client
- log4j

```
<dependencies>
    <!-- https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client -->
    <dependency>
        <groupId>com.amazonaws</groupId>
        <artifactId>amazon-kinesis-client</artifactId>
        <version>1.11.1</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/org.apache.commons/commons-lang3 -->
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-lang3</artifactId>
        <version>3.8.1</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/log4j/log4j -->
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.17</version>
    </dependency>
</dependencies>
```

---

## Recommendations
- You should set the `initialPositionInStream` to `TRIM_HORIZON` in `KinesisConsumerConfig` for consumer startup, shutdown and Throttling. The `TRIM_HORIZON` is for the 1st time when KCL DynamoDB table. Then, KCL based on the latest consumed `sequence number` from the DynamoDB table to consume stream records.
- Your concrete KinesisConsumerHandler must have the ability to handle duplicate records.
- Create another IAM

---

## Permissions

KCL needs these AWS resource permissions. 
- Kinesis:
  - ListShards
  - AmazonKinesisReadOnlyAccess
- DynamoDB:
  - CreateTable
  - DescribeTable
  - GetItem
  - PutItem
- CloudWatch:
  - PutMetricAlarm

---

## References
- Github: 
    - https://github.com/awslabs/amazon-kinesis-client
    - https://github.com/awsdocs/amazon-kinesis-data-streams-developer-guide/blob/master/doc_source/developing-consumers-with-sdk.md
- Maven repo (1.0):
    - https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client 
- Maven repo (2.0):
    - https://mvnrepository.com/artifact/software.amazon.kinesis/amazon-kinesis-client
- Javadoc:
    - https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/kinesis/AmazonKinesisClient.html   
- Developing Consumers Using the Kinesis Client Library 1.x
    - https://docs.aws.amazon.com/en_us/streams/latest/dev/developing-consumers-with-kcl.html
- Developing a Kinesis Client Library Consumer in Java (2.0):
    - https://docs.aws.amazon.com/en_us/streams/latest/dev/kcl2-standard-kinesisStreamConsumer-java-example.html
- Using Consumers with Enhanced Fan-Out (2.0):
    - https://docs.aws.amazon.com/en_us/streams/latest/dev/introduction-to-enhanced-consumers.html

- Docs:
    - https://docs.aws.amazon.com/zh_tw/streams/latest/dev/kinesis-dg.pdf
    - https://docs.aws.amazon.com/en_us/streams/latest/dev/kinesis-record-processor-additional-considerations.html
    - https://docs.aws.amazon.com/en_us/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax
    - https://docs.aws.amazon.com/zh_tw/streams/latest/dev/developing-consumers-with-sdk.html    
    
    
 