package tm.raftel.util.config;

public class ConfigFileNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public ConfigFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
