package flink;


import flink.operator.WordsMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import static flink.kafka.connector.Consumer.createStringConsumer;
import static flink.kafka.connector.Producer.createStringProducer;


public class FlinkDataPipeline {

    public static void length() throws Exception {
        String inputTopic = "test";
        String outputTopic = "test_out";
        String address = "localhost:9092";

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<String> flinkKafkaConsumer =
                createStringConsumer(inputTopic, address);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<String> stringInputStream =
                environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer =
                createStringProducer(outputTopic, address);

        stringInputStream
                .map(new WordsMapper())
                .addSink(flinkKafkaProducer);

        environment.execute();
    }

    public static void main(String[] args) throws Exception {
        length();
    }

}
