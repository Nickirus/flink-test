package flink.schema;

import flink.model.Message;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;


import java.io.IOException;

public class MessageDeserializationSchema implements DeserializationSchema<Message> {

    private static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public Message deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Message.class);
    }

    @Override
    public boolean isEndOfStream(Message message) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}
