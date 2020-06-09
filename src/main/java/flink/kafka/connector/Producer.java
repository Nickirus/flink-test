package flink.kafka.connector;

import flink.model.Message;
import flink.model.Snapshot;
import flink.schema.MessageListSerializationSchema;
import flink.schema.MessageSerializationSchema;
import flink.schema.SnapshotSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.List;

public class Producer {
    public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaProducer011<Message> createMessageProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<>(kafkaAddress, topic, new MessageSerializationSchema());
    }

    public static FlinkKafkaProducer011<List<Message>> createMessageListProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<>(kafkaAddress, topic, new MessageListSerializationSchema());
    }

    public static FlinkKafkaProducer011<Snapshot> createSnapshotProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SnapshotSerializationSchema());
    }
}
