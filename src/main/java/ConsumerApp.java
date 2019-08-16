import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.xml.DOMConfigurator;
import tm.raftel.util.aws_kinesis.KinesisConsumer;
import tm.raftel.util.aws_kinesis.KinesisConsumerBag;
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
            System.err.println(String.format("Fail to start %s. Error -> %s", ConsumerApp.class.getName(), ExceptionUtils.toStackTrace(e)));
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
        String log4j2ConfigFile = String.format("%s/log4j.xml", phase);
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
            Properties config = configUtils.getProperties(String.format("config.properties"));
            // Create bag
            String accessKeyId = configUtils.getProperty(config, "accessKeyId");
            String accessSecretKey = configUtils.getProperty(config, "accessSecretKey");
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessSecretKey);
            AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
            String streamRegion = configUtils.getProperty(config, "streamRegion");
            String streamName = configUtils.getProperty(config, "streamName");
            String consumerName = configUtils.getProperty(config, "consumerName");
            KinesisConsumerBag bag = new KinesisConsumerBag(awsStaticCredentialsProvider, streamRegion, streamName, consumerName);
            // Set bag optional settings
            String initialPositionInStream = configUtils.getProperty(config, "initialPositionInStream");
            bag.setInitialPositionInStream(InitialPositionInStream.valueOf(initialPositionInStream));
            int initialLeaseTableReadCapacity = Integer.parseInt(configUtils.getProperty(config, "initialLeaseTableReadCapacity"));
            bag.setInitialLeaseTableReadCapacity(initialLeaseTableReadCapacity);
            int initialLeaseTableWriteCapacity = Integer.parseInt(configUtils.getProperty(config, "initialLeaseTableWriteCapacity"));
            bag.setInitialLeaseTableWriteCapacity(initialLeaseTableWriteCapacity);
            int maxPollRecordCount = Integer.parseInt(configUtils.getProperty(config, "maxPollRecordCount"));
            bag.setMaxPollRecordCount(maxPollRecordCount);
            long processRetryDelayMillis = Long.parseLong(configUtils.getProperty(config, "processRetryDelayMillis"));
            bag.setProcessRetryDelayMillis(processRetryDelayMillis);
            int checkpointMaxRetryCount = Integer.parseInt(configUtils.getProperty(config, "checkpointMaxRetryCount"));
            bag.setCheckpointMaxRetryCount(checkpointMaxRetryCount);
            long checkpointRetryDelayMillis = Long.parseLong(configUtils.getProperty(config, "checkpointRetryDelayMillis"));
            bag.setCheckpointRetryDelayMillis(checkpointRetryDelayMillis);
            String recordDataDecoderCharset = configUtils.getProperty(config, "recordDataDecoder");
            bag.setRecordDataDecoder(Charset.forName(recordDataDecoderCharset).newDecoder());
            // Create and start KinesisConsumer
            KinesisConsumer kinesisConsumer = new KinesisConsumer(bag);
            kinesisConsumer.start();
        } catch (Exception e) {
            throw new AppFatalException("Fail to start consumer.", e);
        }
    }
}
