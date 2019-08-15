package tm.raftel.util.phase;

public class PhaseUtils {

    private static PhaseUtils instance = new PhaseUtils();
    private String phase = "";

    private PhaseUtils() {
    }

    public static PhaseUtils build() {
        return instance;
    }

    public String getPhase() {
        return phase;
    }

    public void setPhase(String phase) {
        this.phase = phase;
    }
}
