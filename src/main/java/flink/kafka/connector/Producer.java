package flink.kafka.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Producer {
    public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SimpleStringSchema());
    }
}
