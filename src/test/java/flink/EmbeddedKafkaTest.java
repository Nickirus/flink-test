package flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {FlinkDataPipeline.class, DataGenerator.class})
@DirtiesContext
public class EmbeddedKafkaTest {

    @Autowired
    FlinkDataPipeline flinkDataPipeline;

    @Autowired
    DataGenerator dataGenerator;

    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "test_out";

    private KafkaMessageListenerContainer<String, String> containerInput;
    private KafkaMessageListenerContainer<String, String> containerOutput;
    private BlockingQueue<ConsumerRecord<String, String>> recordsInput;
    private BlockingQueue<ConsumerRecord<String, String>> recordsOutput;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka =
            new EmbeddedKafkaRule(1, true, INPUT_TOPIC, OUTPUT_TOPIC).kafkaPorts(9092);

    @Before
    public void setUp() throws Exception {
        //  сообщения принятые консьюмером
        recordsInput = new LinkedBlockingQueue<>();
        recordsOutput = new LinkedBlockingQueue<>();

        // создаем фабрику консьюмера
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("flink", "true", embeddedKafka.getEmbeddedKafka());
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);

        // задаем проперти для консьюмеров
        ContainerProperties containerPropertiesInput = new ContainerProperties(INPUT_TOPIC);
        ContainerProperties containerPropertiesOutput = new ContainerProperties(OUTPUT_TOPIC);

        // передаем проперти в консьюмеров
        containerInput = new KafkaMessageListenerContainer<>(consumerFactory, containerPropertiesInput);
        containerOutput = new KafkaMessageListenerContainer<>(consumerFactory, containerPropertiesOutput);

        // обработчик сообщений топика
        containerInput.setupMessageListener((MessageListener<String, String>) record -> recordsInput.add(record));
        containerOutput.setupMessageListener((MessageListener<String, String>) record -> recordsOutput.add(record));

        // запустили консьюмеров(слушателей топика)
        containerInput.start();
        containerOutput.start();

        // ожидание распределения  партиций по группам консьюмеров
        ContainerTestUtils.waitForAssignment(containerInput, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
        ContainerTestUtils.waitForAssignment(containerOutput, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        containerOutput.stop();
        containerInput.stop();
    }

    @Test
    public void test() throws Exception {

        Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getEmbeddedKafka().getBrokersAsString());

        ProducerFactory<Integer, String> producer = new DefaultKafkaProducerFactory<>(senderProps);

        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producer);
        template.setDefaultTopic(INPUT_TOPIC);

        String message = "{\"sender\":\"User03\",\"recipient\":\"User1\",\"uuid\":\"6e0f101c-f1a6-4403-aa26-ccb3c83a9dd2\",\"message\":\"Hi, how are you - 3?\",\"sum\":1,\"sentAt\":[2020,6,22,14,25,58,237000000]}";

        template.sendDefault(message);

        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        flinkDataPipeline.simplePipelineWithKeySelector(environment);

        ConsumerRecord<String, String> receivedInput = recordsInput.poll(15, SECONDS);
        ConsumerRecord<String, String> receivedOutput = recordsOutput.poll(15, SECONDS);

        assertThat(receivedInput, hasValue(message));
        assertTrue(receivedOutput.value().contains("user-User03"));
    }
}


