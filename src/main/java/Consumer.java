import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import k0.util.aws_kinesis.KinesisConsumer;
import k0.util.aws_kinesis.KinesisConsumerConfig;
import k0.util.config.ConfigFileNotFoundException;
import k0.util.config.ConfigNotFoundException;
import k0.util.config.ConfigUtils;
import k0.util.exception.ExceptionUtils;
import k0.util.phase.PhaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.xml.DOMConfigurator;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

public class Consumer {

    private String phase;

    public static void main(String[] args) {
        Consumer app = new Consumer();
        try {
            app.setupPhase(args);
            app.setupLogging();
            app.start();
        } catch (Exception e) {
            System.err.println(String.format("Fail to start %s. Error -> %s", Consumer.class.getName(), ExceptionUtils.toStackTrace(e)));
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

    private void setupLogging() throws AppFatalException {
        String phase = PhaseUtils.build().getPhase();
        String loggingConfigFile = String.format("%s/k0.util.aws_kinesis-log4j.xml", phase);
        String exceptionMessage = String.format("Cannot load log4j config file: %s", loggingConfigFile);
        try {
            URL log4j2ConfigFileUrl = Consumer.class.getClassLoader().getResource(loggingConfigFile);
            if (log4j2ConfigFileUrl == null) {
                throw new AppFatalException(exceptionMessage);
            }
            DOMConfigurator.configure(log4j2ConfigFileUrl);
        } catch (Exception e) {
            throw new AppFatalException(exceptionMessage, e);
        }
    }

    private void start() throws AppFatalException {
        try {
            KinesisConsumerConfig consumerConfig = createKinesisConsumerConfig();
            ConsumerHelper consumeHelper = new ConsumerHelper();
            KinesisConsumer kinesisConsumer = new KinesisConsumer(consumerConfig, consumeHelper);
            kinesisConsumer.subscribeStream();
        } catch (Exception e) {
            throw new AppFatalException("Fail to start consumer.", e);
        }
    }

    private KinesisConsumerConfig createKinesisConsumerConfig() throws ConfigFileNotFoundException, ConfigNotFoundException {
        // Load settings from config file
        ConfigUtils configUtils = ConfigUtils.build();
        Properties configFile = configUtils.getProperties(String.format("config.properties"));
        String accessKeyId = configUtils.getProperty(configFile, "streamAccessKeyId");
        String accessSecretKey = configUtils.getProperty(configFile, "streamAccessSecretKey");
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        String streamRegion = configUtils.getProperty(configFile, "streamRegion");
        String streamName = configUtils.getProperty(configFile, "streamName");
        String consumerName = configUtils.getProperty(configFile, "consumerName");
        // Create KinesisConsumerConfig
        KinesisConsumerConfig consumerConfig = new KinesisConsumerConfig(awsStaticCredentialsProvider, streamRegion, streamName, consumerName);
        String initialPositionInStream = configUtils.getProperty(configFile, "initialPositionInStream");
        consumerConfig.setInitialPositionInStream(InitialPositionInStream.valueOf(initialPositionInStream));
        int initialLeaseTableReadCapacity = Integer.parseInt(configUtils.getProperty(configFile, "initialLeaseTableReadCapacity"));
        consumerConfig.setInitialLeaseTableReadCapacity(initialLeaseTableReadCapacity);
        int initialLeaseTableWriteCapacity = Integer.parseInt(configUtils.getProperty(configFile, "initialLeaseTableWriteCapacity"));
        consumerConfig.setInitialLeaseTableWriteCapacity(initialLeaseTableWriteCapacity);
        int maxPollRecordCount = Integer.parseInt(configUtils.getProperty(configFile, "maxPollRecordCount"));
        consumerConfig.setMaxPollRecordCount(maxPollRecordCount);
        long consumerRestartDelayMillis = Long.parseLong(configUtils.getProperty(configFile, "consumerRestartDelayMillis"));
        consumerConfig.setConsumerRestartDelayMillis(consumerRestartDelayMillis);
        int checkpointMaxRetryCount = Integer.parseInt(configUtils.getProperty(configFile, "checkpointMaxRetryCount"));
        consumerConfig.setCheckpointMaxRetryCount(checkpointMaxRetryCount);
        long checkpointRetryDelayMillis = Long.parseLong(configUtils.getProperty(configFile, "checkpointRetryDelayMillis"));
        consumerConfig.setCheckpointRetryDelayMillis(checkpointRetryDelayMillis);
        String recordDataDecoderCharset = configUtils.getProperty(configFile, "recordDataDecoder");
        consumerConfig.setRecordDataDecoder(Charset.forName(recordDataDecoderCharset).newDecoder());
        return consumerConfig;
    }
}
