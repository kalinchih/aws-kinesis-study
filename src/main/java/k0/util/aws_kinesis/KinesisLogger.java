package k0.util.aws_kinesis;

import k0.util.date.DateUtils;
import k0.util.exception.ExceptionUtils;
import k0.util.json.JsonUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class KinesisLogger {

    private static KinesisLogger instance = new KinesisLogger();
    private boolean enableInfoLog = false;
    private Logger infoLogger = Logger.getLogger(String.format("%s.InfoLogger",
            KinesisLogger.class.getPackage().getName()));
    private Logger errorLogger = Logger.getLogger(String.format("%s.ErrorLogger",
            KinesisLogger.class.getPackage().getName()));

    private KinesisLogger() {
    }

    public static KinesisLogger build(boolean enableInfoLog) {
        instance.enableInfoLog = enableInfoLog;
        return instance;
    }

    public boolean isEnableInfoLog() {
        return enableInfoLog;
    }

    public void setEnableInfoLog(boolean enableInfoLog) {
        this.enableInfoLog = enableInfoLog;
    }

    public void debug(String message) {
        if (enableInfoLog) {
            infoLogger.debug(wrapLogData("debug", message, null));
        }
    }

    public void info(String message) {
        if (enableInfoLog) {
            infoLogger.info(wrapLogData("info", message, null));
        }
    }

    public void warn(Throwable exception) {
        errorLogger.warn(wrapLogData("warn", exception.getMessage(), exception));
    }

    public void error(Throwable exception) {
        errorLogger.error(wrapLogData("error", exception.getMessage(), exception));
    }

    public void fatal(Throwable exception) {
        errorLogger.fatal(wrapLogData("fatal", exception.getMessage(), exception));
    }

    private String wrapLogData(String level, String message, Throwable exception) {
        Map<String, Object> logData = new HashMap<String, Object>();
        logData.put("time", DateUtils.getUtcNowText());
        logData.put("level", level);
        logData.put("message", message);
        if (exception != null) {
            logData.put("exception", ExceptionUtils.toStackTrace(exception));
        }
        String logDataString = JsonUtils.build().toJson(logData);
        return logDataString;
    }
}
