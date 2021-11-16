package Utils;

import CustomDataTypes.ShortOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public class ObjectSerializationSchema implements SerializationSchema<Tuple2<Long, List<Tuple2<Integer, Long>>>> {

    private ObjectMapper mapper;

    @Override
    public byte[] serialize(Tuple2<Long, List<Tuple2<Integer, Long>>> obj) {
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
