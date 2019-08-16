package k0.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtils {

    private static JsonUtils instance = new JsonUtils();
    private ObjectMapper objectMapper = new ObjectMapper();

    private JsonUtils() {
        objectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    }

    public static JsonUtils build() {
        return instance;
    }

    public String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            return null;
        }
    }

    public JsonNode toJsonNode(String jsonText) {
        try {
            return objectMapper.readTree(jsonText);
        } catch (Exception e) {
            return null;
        }
    }

    public <T> T toObject(String content, Class<T> clazz) {
        try {
            return objectMapper.readValue(content, clazz);
        } catch (Exception e) {
            return null;
        }
    }

    public <T> T toObject(JsonNode jsonNode, Class<T> clazz) {
        try {
            return objectMapper.treeToValue(jsonNode, clazz);
        } catch (Exception e) {
            return null;
        }
    }
}
