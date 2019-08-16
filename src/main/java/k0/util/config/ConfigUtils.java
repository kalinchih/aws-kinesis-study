package k0.util.config;

import k0.util.phase.PhaseUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigUtils {

    private static ConfigUtils instance = new ConfigUtils();
    private Map<String, Properties> configPropertiesCache = new HashMap<String, Properties>();

    private ConfigUtils() {
    }

    public static ConfigUtils build() {
        return instance;
    }

    public Properties getProperties(String fileName) throws ConfigFileNotFoundException {
        if (configPropertiesCache.containsKey(fileName)) {
            return configPropertiesCache.get(fileName);
        } else {
            String tempName = null;
            Properties configProperties = new Properties();
            if (StringUtils.isNotBlank(PhaseUtils.build().getPhase())) {
                tempName = PhaseUtils.build().getPhase() + "/" + fileName;
            } else {
                tempName = fileName;
            }
            InputStream inputStream = getClass().getResourceAsStream("/" + tempName);
            try {
                configProperties.load(inputStream);
            } catch (IOException | NullPointerException e) {
                throw new ConfigFileNotFoundException("Properties file (" + fileName + ") not found." + fileName, e);
            }
            configPropertiesCache.put(fileName, configProperties);
            return configProperties;
        }
    }

    public String getProperty(Properties properties, String key) throws ConfigNotFoundException {
        String keyValue = null;
        if (properties.containsKey(key)) {
            keyValue = properties.getProperty(key);
        } else {
            throw new ConfigNotFoundException("Property (" + key + ") not found.");
        }
        return keyValue;
    }
}
