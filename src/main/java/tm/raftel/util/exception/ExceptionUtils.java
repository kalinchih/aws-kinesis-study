package tm.raftel.util.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

public class ExceptionUtils {

    public static String toStackTrace(Throwable cause) {
        String causeString = "";
        if (cause != null) {
            Throwable rootCause = cause.getCause();
            if (rootCause != null) {
                try (StringWriter stringWriter = new StringWriter(); PrintWriter printWriter = new PrintWriter(stringWriter)) {
                    rootCause.printStackTrace(printWriter);
                    causeString = stringWriter.toString();
                } catch (IOException e) {
                    rootCause.addSuppressed(e);
                    String.format("Failed to close StringWriter or PrinterWriter after printing stack trace. Stackstrace: %s. %s",
                            rootCause.getMessage(), Arrays.toString(rootCause.getStackTrace()));
                }
            } else {
                causeString = String.format("%s.%s", cause.getMessage(), Arrays.toString(cause.getStackTrace()));
            }
        }
        return causeString;
    }
}
