package flink;

import flink.model.Message;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.stereotype.Component;

import static flink.kafka.connector.Producer.createMessageProducer;

@Component
public class DataGenerator {

    private static final String OUTPUT_TOPIC = "test";
    private static final String ADDRESS = "127.0.0.1:9092";

    public void generate() throws Exception {

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Message> messageStream = environment.addSource(new SourceFunction<Message>() {
            private static final long serialVersionUID = 6369260445318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<Message> context) throws Exception {
                long i = 0;
                final long timeout = 5000;
                while (this.running) {
                    Message message = new Message();
                    if(i<2) {
                        message.setSender("User0");
                    } else {
                        message.setSender("User0"+i);
                    }
                    message.setRecipient("User1");
                    message.setSum(1);
                    message.setMessage(String.format("Hi, how are you - %s?", i++));
                    context.collect(message);
//                    context.collect(String.format("{\"sender\": \"User0\",\"recipient\": \"User1\",\"message\": \"Hi, how are you - %s?\", \"sum\":\"4\"}", i++));
                    Thread.sleep(timeout);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        FlinkKafkaProducer011<Message> flinkKafkaProducer =
                createMessageProducer(OUTPUT_TOPIC, ADDRESS);
        messageStream.addSink(flinkKafkaProducer);

        environment.execute("Write messages into Kafka");
    }

//    public static void main(String[] args) throws Exception {
//        generate();
//    }
}

