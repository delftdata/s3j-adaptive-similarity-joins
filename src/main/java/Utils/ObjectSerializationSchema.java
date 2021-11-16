package Utils;

import CustomDataTypes.ShortOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ObjectSerializationSchema<T> implements KafkaSerializationSchema<T> {

    private ObjectMapper mapper;
    private final String topic;
    private final String key;

    public ObjectSerializationSchema(String key, String topic){
        super();
        this.key = key;
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T obj, Long timestamp) {
        byte[] b = null;
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            b= mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        return new ProducerRecord<>(topic, key.getBytes(), b);
    }

}
