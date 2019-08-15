public class AppFatalException extends Exception {

    private static final long serialVersionUID = 2500295904792888875L;

    public AppFatalException(String message) {
        super(message);
    }

    public AppFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
