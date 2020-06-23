package flink;


import flink.model.Message;
import flink.model.Snapshot;
import flink.operator.CollapseAggregator;
import flink.operator.InfoAggregator;
import flink.operator.MessageTimestampAssigner;
import flink.operator.SnapshotAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static flink.kafka.connector.Consumer.createMessageConsumer;
import static flink.kafka.connector.Producer.*;

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/libs/cep.html
 */

@SuppressWarnings("serial")
@Component
public class FlinkDataPipeline {

    static final Logger log = LoggerFactory.getLogger(FlinkDataPipeline.class);

    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "test_out";
    private static final String ADDRESS = "127.0.0.1:9092";

    private static void simplePipeline() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Для просмотра веб-морды flink на http://localhost:8081
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
                    public void flatMap(Message message, Collector<String> out) {
                        for (String word : message.getMessage().split(" ")) {
                            out.collect(word);
                        }
                    }
                })
//                .map(new WordsMapper())
                .addSink(flinkKafkaProducer);


        environment.execute("Flink Data Pipeline");
    }

    public JobClient simplePipelineWithKeySelector(StreamExecutionEnvironment environment) throws Exception {

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

        return environment.executeAsync("Flink Data Pipeline");
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

    //Агрегация с дизъюнкцией событий по ключам
    public JobClient logicalAdditionByKeysWithTimeWindow(StreamExecutionEnvironment environment) throws Exception {
        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromLatest();

        FlinkKafkaProducer011<Snapshot> flinkKafkaProducer = createSnapshotProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        messagesStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(15)))
                .aggregate(new CollapseAggregator())
                .addSink(flinkKafkaProducer);

        return environment.executeAsync("Flink Data Pipeline");
    }

    private static void pipelineWithProcessTime() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        flinkKafkaConsumer.setStartFromLatest();

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

    private static void pipelineWithSessionWindow() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        //Для считывания с последнего offset
        flinkKafkaConsumer.setStartFromLatest();

        FlinkKafkaProducer011<Snapshot> flinkKafkaProducer = createSnapshotProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);

        //Определение сессионного gap в зависимости от отправителя
        messagesStream
                .keyBy("sender")
                .window(ProcessingTimeSessionWindows.withDynamicGap((message) -> message.getSender().equals("User0")
                        ? 10000 : 12000))
                .aggregate(new SnapshotAggregator())
                .addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

    private static void pipelineWithPattern() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = createMessageConsumer(INPUT_TOPIC, ADDRESS);
        //Для считывания с последнего offset
        flinkKafkaConsumer.setStartFromLatest();

        FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(OUTPUT_TOPIC, ADDRESS);

        DataStream<Message> messagesStream = environment.addSource(flinkKafkaConsumer);
        Pattern<Message, ?> pattern = Pattern.<Message>begin("start").where(
                new SimpleCondition<Message>() {
                    @Override
                    public boolean filter(Message event) {
                        return event.getSum() == 0;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<Message>() {
                    @Override
                    public boolean filter(Message event) {
                        return event.getSum() == 1;
                    }
                }
        ).next("end").where(
                new SimpleCondition<Message>() {
                    @Override
                    public boolean filter(Message event) {
                        return event.getSum() == 2;
                    }
                }
        );

        PatternStream<Message> patternStream = CEP.pattern(messagesStream, pattern);

        patternStream.select(
                (PatternSelectFunction<Message, String>) map -> "Detected!").addSink(flinkKafkaProducer);

        environment.execute("Flink Data Pipeline");
    }

//    public static void main(String[] args) throws Exception {
////        simplePipeline();//#1
////        simplePipelineWithKeySelector();//#2
////        reducePipeline();//#3
////        aggregationWithTimeWindow();//#4
//        logicalAdditionByKeysWithTimeWindow();//#5
////        pipelineWithProcessTime();//#6
////        pipelineWithEventTime();//#7
////        pipelineWithSessionWindow();//#8
////        pipelineWithPattern();//#9
//    }

}
