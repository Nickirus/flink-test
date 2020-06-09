package flink;


import flink.model.Message;
import flink.model.Snapshot;
import flink.operator.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.List;

import static flink.kafka.connector.Consumer.createMessageConsumer;
import static flink.kafka.connector.Producer.*;

@SuppressWarnings("serial")
public class FlinkDataPipeline {

    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "test_out";
    private static final String ADDRESS = "localhost:9092";

    private static void easyPipeline() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //Для просмотра веб-морды на http://localhost:8081
//        StreamExecutionEnvironment environment;
//        Configuration conf = new Configuration();
//        environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<Message> messagesStream =
                environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer =
            createStringProducer(OUTPUT_TOPIC, ADDRESS);

        //Дедупликация по заданным ключам, фильтрация, маппинг в читаемую строку
        messagesStream
                .keyBy("sender", "recipient", "message").flatMap(new DuplicateFilter())
                .filter((FilterFunction<Message>) value -> value.getSum() != null)
                .map(new WordsMapper())
                .addSink(flinkKafkaProducer);


        environment.execute("Flink Data Pipeline");
}

    private static void aggregationWithTimeWindow() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();


        DataStream<Message> messagesStream =
                environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer =
                createStringProducer(OUTPUT_TOPIC, ADDRESS);

        //Агрегация по пользователям во временном окне
        messagesStream
                .keyBy("sender")
                .timeWindow(Time.seconds(15))
                .aggregate(new InfoAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void deduplicateWithTimeWindow() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer011<List<Message>> flinkKafkaProducer =
                createMessageListProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        //Дедупликация только в рамках временного окна
        messagesStream
                .timeWindowAll(Time.seconds(10))
                .aggregate(new CollapseAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void pipelineWithProcessTime() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer011<Snapshot> flinkKafkaProducer = createSnapshotProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        //Создание снепшотов по системному времени
        messagesStream
                .timeWindowAll(Time.seconds(15))
                .aggregate(new SnapshotAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void pipelineWithEventTime() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer =
                createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        //откуда брать EventTime и определение Watermark
        flinkKafkaConsumer.assignTimestampsAndWatermarks(new MessageTimestampAssigner());

        FlinkKafkaProducer011<Snapshot> flinkKafkaProducer = createSnapshotProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        //Создание снепшотов по времени самих событий
        messagesStream
                .timeWindowAll(Time.seconds(15))
                .aggregate(new SnapshotAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }



    public static void main(String[] args) throws Exception {
//        easyPipeline();
//        easyPipeline();
//        deduplicateWithTimeWindow();
//        pipelineWithProcessTime();
        pipelineWithEventTime();
    }

}
