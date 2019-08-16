package k0.util.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateUtils {

    public final static ZoneId ZONE_ID_UTC = ZoneId.of("UTC");
    public final static ZoneId ZONE_ID_AMERICA_LOS_ANGELES = ZoneId.of("America/Los_Angeles");
    public final static ZoneId ZONE_ID_TOKYO = ZoneId.of("Asia/Tokyo");
    public final static DateTimeFormatter FORMATTER_ISO_8601 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private DateUtils() {
    }

    /**
     * Get UTC now in ISO_INSTANT format (2017-05-01T22:55:00.873Z).
     *
     * @return UTC now text
     */
    public static String getUtcNowText() {
        ZonedDateTime zonedDateTime = Instant.now().atZone(ZONE_ID_UTC);
        return zonedDateTime.format(DateTimeFormatter.ISO_INSTANT);
    }

    /**
     * @param dateTimeFormatter DateTimeFormatter
     * @return UTC now text
     * @see
     * <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a>
     */
    public static String getUtcNowText(DateTimeFormatter dateTimeFormatter) {
        ZonedDateTime zonedDateTime = Instant.now().atZone(ZONE_ID_UTC);
        return zonedDateTime.format(dateTimeFormatter);
    }

    public static long getDiffMinutes(long epochMs1, long epochMs2) {
        long diffMinutes = ChronoUnit.MINUTES.between(Instant.ofEpochMilli(epochMs1), Instant.ofEpochMilli(epochMs2));
        return diffMinutes;
    }

    /**
     * @param zoneId            ZoneId
     * @param dateTimeFormatter DateTimeFormatter
     * @return UTC now text
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html">TimeZone</a>
     * @see
     * <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a>
     */
    public static String getNowText(ZoneId zoneId, DateTimeFormatter dateTimeFormatter) {
        ZonedDateTime zonedDateTime = Instant.now().atZone(zoneId);
        return zonedDateTime.format(dateTimeFormatter);
    }

    public static long getEpochMs() {
        return Instant.now().toEpochMilli();
    }
}


