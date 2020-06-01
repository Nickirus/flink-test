package flink.kafka.connector;

import flink.model.Message;
import flink.schema.MessageDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Consumer {
    public static FlinkKafkaConsumer011<Message> createMessageConsumer(String topic, String kafkaAddress) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);

        return new FlinkKafkaConsumer011<>(topic, new MessageDeserializationSchema(), props);
    }
}
