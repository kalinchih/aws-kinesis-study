package k0.util.random;

import org.apache.commons.lang3.StringUtils;

import java.util.Random;
import java.util.UUID;

public class RandomUtils {

    public static final String DASH = "-";

    private RandomUtils() {
    }

    public static String getUuid() {
        return StringUtils.remove(UUID.randomUUID().toString(), DASH);
    }

    public static int getRandom(int bound) {
        return new Random().nextInt(bound);
    }
}
