import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.xml.DOMConfigurator;
import tm.raftel.util.aws_kinesis.KinesisConsumer;
import tm.raftel.util.aws_kinesis.KinesisConsumerConfig;
import tm.raftel.util.config.ConfigNotFoundException;
import tm.raftel.util.config.ConfigUtils;
import tm.raftel.util.exception.ExceptionUtils;
import tm.raftel.util.phase.PhaseUtils;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

public class ConsumerApp {

    private String phase;

    public static void main(String[] args) {
        ConsumerApp app = new ConsumerApp();
        try {
            app.setupPhase(args);
            app.setupLog4j2();
            app.pollStream();
        } catch (Exception e) {
            System.err.println(String.format("Fail to start %s. Error -> %s", ConsumerApp.class.getName(),
                    ExceptionUtils.toStackTrace(e)));
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

    private void setupLog4j2() throws AppFatalException {
        String phase = PhaseUtils.build().getPhase();
        String log4j2ConfigFile = String.format("%s/tm.raftel.util.aws_kinesis-log4j.xml", phase);
        String exceptionMessage = String.format("Cannot load log4j config file: %s", log4j2ConfigFile);
        try {
            URL log4j2ConfigFileUrl = ConsumerApp.class.getClassLoader().getResource(log4j2ConfigFile);
            if (log4j2ConfigFileUrl == null) {
                throw new AppFatalException(exceptionMessage);
            }
            DOMConfigurator.configure(log4j2ConfigFileUrl);
        } catch (Exception e) {
            throw new AppFatalException(exceptionMessage, e);
        }
    }

    private void pollStream() throws AppFatalException {
        try {
            // Load settings from config file
            ConfigUtils configUtils = ConfigUtils.build();
            Properties configFile = configUtils.getProperties(String.format("config.properties"));
            String accessKeyId = configUtils.getProperty(configFile, "accessKeyId");
            String accessSecretKey = configUtils.getProperty(configFile, "accessSecretKey");
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
            AWSStaticCredentialsProvider awsStaticCredentialsProvider =
                    new AWSStaticCredentialsProvider(awsCredentials);
            String streamRegion = configUtils.getProperty(configFile, "streamRegion");
            String streamName = configUtils.getProperty(configFile, "streamName");
            String consumerName = configUtils.getProperty(configFile, "consumerName");
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
        } catch (Exception e) {
            throw new AppFatalException("Fail to start consumer.", e);
        }
    }

    private void setOptionalSettings(ConfigUtils configUtils, Properties configFile,
            KinesisConsumerConfig consumerConfig) throws ConfigNotFoundException {
        String initialPositionInStream = configUtils.getProperty(configFile, "initialPositionInStream");
        consumerConfig.setInitialPositionInStream(InitialPositionInStream.valueOf(initialPositionInStream));
        int initialLeaseTableReadCapacity = Integer.parseInt(configUtils.getProperty(configFile,
                "initialLeaseTableReadCapacity"));
        consumerConfig.setInitialLeaseTableReadCapacity(initialLeaseTableReadCapacity);
        int initialLeaseTableWriteCapacity = Integer.parseInt(configUtils.getProperty(configFile,
                "initialLeaseTableWriteCapacity"));
        consumerConfig.setInitialLeaseTableWriteCapacity(initialLeaseTableWriteCapacity);
        int maxPollRecordCount = Integer.parseInt(configUtils.getProperty(configFile, "maxPollRecordCount"));
        consumerConfig.setMaxPollRecordCount(maxPollRecordCount);
        long consumerRestartDelayMillis = Long.parseLong(configUtils.getProperty(configFile,
                "consumerRestartDelayMillis"));
        consumerConfig.setConsumerRestartDelayMillis(consumerRestartDelayMillis);
        int checkpointMaxRetryCount = Integer.parseInt(configUtils.getProperty(configFile, "checkpointMaxRetryCount"));
        consumerConfig.setCheckpointMaxRetryCount(checkpointMaxRetryCount);
        long checkpointRetryDelayMillis = Long.parseLong(configUtils.getProperty(configFile,
                "checkpointRetryDelayMillis"));
        consumerConfig.setCheckpointRetryDelayMillis(checkpointRetryDelayMillis);
        String recordDataDecoderCharset = configUtils.getProperty(configFile, "recordDataDecoder");
        consumerConfig.setRecordDataDecoder(Charset.forName(recordDataDecoderCharset).newDecoder());
        boolean enableInfoLog = Boolean.parseBoolean(configUtils.getProperty(configFile, "enableInfoLog"));
        consumerConfig.setEnableInfoLog(enableInfoLog);
    }
}
