package flink;


import flink.model.Message;
import flink.model.Snapshot;
import flink.operator.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static flink.kafka.connector.Consumer.createMessageConsumer;
import static flink.kafka.connector.Producer.*;

@SuppressWarnings("serial")
public class FlinkDataPipeline {

    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "test_out";
    private static final String ADDRESS = "localhost:9092";

    private static void simplePipeline() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Для просмотра веб-морды на http://localhost:8081
//        StreamExecutionEnvironment environment;
//        Configuration conf = new Configuration();
//        environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(OUTPUT_TOPIC, ADDRESS);

        //Дедупликация по заданным ключам, фильтрация, маппинг в [читаемую строку]/[сообщения в отдельные слова]
        messagesStream
                .keyBy("sender", "recipient", "message")
//                .flatMap(new DuplicateFilter())
//                .filter((FilterFunction<Message>) value -> value.getSum() != null)
                .flatMap(new FlatMapFunction<Message, String>() {
                    @Override
                    public void flatMap(Message message, Collector<String> out)
                            throws Exception {
                        for (String word : message.getMessage().split(" ")) {
                            out.collect(word);
                        }
                    }
                })
//                .map(new WordsMapper())
                .addSink(flinkKafkaProducer);


        environment.execute("Flink Data Pipeline");
    }

    private static void simplePipelineWitKeySelector() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(OUTPUT_TOPIC, ADDRESS);

        //Аггрегация по динамичному набору ключей в зависимости от суммы
        messagesStream
                .keyBy((KeySelector<Message, Object>)
                        message -> message.getSum() == 0
                                ? message.getSender()
                                : new Tuple2<>(message.getSender(), message.getRecipient()))
                .timeWindow(Time.seconds(15))
                .aggregate(new InfoAggregator())
                .addSink(flinkKafkaProducer);


        environment.execute("Flink Data Pipeline");
    }

    private static void reducePipeline() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);

        //Для считывания с последнего offset
        flinkKafkaConsumer.setStartFromLatest();

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<Message> flinkKafkaProducer = createMessageProducer(OUTPUT_TOPIC, ADDRESS);

        //В рамках временного окна редуцирование сообщений по ключу с сохранением суммарной суммы
        messagesStream
                .keyBy("sender","recipient")
                .timeWindow(Time.seconds(15))
                .reduce(new ReduceFunction<Message>() {
                    private static final long serialVersionUID = 1L;
                    public Message reduce(Message message1, Message message2) {
                        Message reducedMessage = new Message();
                        reducedMessage.setSender(message1.getSender());
                        reducedMessage.setRecipient(message1.getRecipient());
                        reducedMessage.setSum(message1.getSum() + message2.getSum());
                        return reducedMessage;
                    }
                })
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void aggregationWithTimeWindow() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);

        //Для считывания с последнего offset
        flinkKafkaConsumer.setStartFromLatest();

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(OUTPUT_TOPIC, ADDRESS);

        //Агрегация по пользователям во временном окне
        messagesStream
                .keyBy("sender")
                .timeWindow(Time.seconds(15))
                .aggregate(new InfoAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void deduplicateWithTimeWindow() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer011<List<Message>> flinkKafkaProducer = createMessageListProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        //Дедупликация только в рамках временного окна
        messagesStream
                .timeWindowAll(Time.seconds(10))
                .aggregate(new CollapseAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void pipelineWithProcessTime() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
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
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
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
        simplePipeline();
//        reducePipeline();
//        simplePipelineWitKeySelector();
//        aggregationWithTimeWindow();
//        deduplicateWithTimeWindow();
//        pipelineWithProcessTime();
//        pipelineWithEventTime();
    }

}
