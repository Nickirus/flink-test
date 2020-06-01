package flink.kafka.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Consumer {
    public static FlinkKafkaConsumer011<String> createStringConsumer(
            String topic, String kafkaAddress) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);

        return new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
    }
}
