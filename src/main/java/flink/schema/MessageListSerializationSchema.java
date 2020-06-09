package flink.schema;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import flink.model.Message;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MessageListSerializationSchema implements SerializationSchema<List<Message>> {

    private static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private Logger logger = LoggerFactory.getLogger(MessageListSerializationSchema.class);

    @Override
    public byte[] serialize(List<Message> messages) {
        try {
            String json = "";
            for (Message message : messages) {

                if (objectMapper == null) {
                    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
                    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
                }
                json = json.concat("\n").concat(objectMapper.writeValueAsString(message));
            }
            return json.getBytes();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }
}
