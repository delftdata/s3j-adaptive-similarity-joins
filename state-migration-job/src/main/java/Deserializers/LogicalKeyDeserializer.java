package Deserializers;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.flink.api.java.tuple.Tuple3;

import java.lang.reflect.Type;

public class LogicalKeyDeserializer implements JsonDeserializer<Tuple3<Integer, Integer, Integer>> {
    @Override
    public Tuple3<Integer, Integer, Integer> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext ctx) throws JsonParseException {
        String[] subElements = jsonElement.getAsString()
                .replace("(","")
                .replace(")", "")
                .split(", ");
        return new Tuple3<>(
                Integer.valueOf(subElements[0]),
                Integer.valueOf(subElements[1]),
                Integer.valueOf(subElements[2]));
    }
}
