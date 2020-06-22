package flink.bootstrap;

import flink.DataGenerator;
import flink.FlinkDataPipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class BootStrapFlink implements CommandLineRunner {

    private final DataGenerator dataGenerator;
    private final FlinkDataPipeline flinkDataPipeline;
    private final StreamExecutionEnvironment env;

    public BootStrapFlink(DataGenerator dataGenerator, FlinkDataPipeline flinkDataPipeline) {
        this.dataGenerator = dataGenerator;
        this.flinkDataPipeline = flinkDataPipeline;
        this.env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    }

    @Override
    public void run(String... args) throws Exception {
        dataGenerator.generate();
        flinkDataPipeline.simplePipelineWithKeySelector(env);
    }
}
