package tm.raftel.util.log;

import org.apache.log4j.Logger;
import tm.raftel.util.date.DateUtils;
import tm.raftel.util.exception.ExceptionUtils;
import tm.raftel.util.json.JsonUtils;

import java.util.HashMap;
import java.util.Map;

public class LogUtils {

    private static LogUtils instance = new LogUtils();
    private Logger infoLogger = Logger.getLogger("info_logger");
    private Logger errorLogger = Logger.getLogger("error_logger");

    private LogUtils() {
    }

    public static LogUtils build() {
        return instance;
    }

    public void debug(String message) {
        infoLogger.debug(wrapLogData("debug", message, null));
    }

    public void info(String message) {
        infoLogger.info(wrapLogData("info", message, null));
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
