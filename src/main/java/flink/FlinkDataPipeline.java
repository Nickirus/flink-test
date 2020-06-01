package flink;


import flink.model.Message;
import flink.operator.WordsMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import static flink.kafka.connector.Consumer.createMessageConsumer;
import static flink.kafka.connector.Producer.createStringProducer;


public class FlinkDataPipeline {

    private static void length() throws Exception {
        String inputTopic = "test";
        String outputTopic = "test_out";
        String address = "localhost:9092";

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(inputTopic, address);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<Message> inputMessagesStream =
                environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer =
                createStringProducer(outputTopic, address);

        inputMessagesStream
                .map(new WordsMapper())
                .addSink(flinkKafkaProducer);

        environment.execute();
    }

    public static void main(String[] args) throws Exception {
        length();
    }

}
