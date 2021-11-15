package Utils;

import CustomDataTypes.ShortOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ObjectSerializationSchema implements SerializationSchema<ShortOutput> {

    private ObjectMapper mapper;

    @Override
    public byte[] serialize(ShortOutput obj) {
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
        return b;
    }

}
