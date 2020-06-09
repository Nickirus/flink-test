package flink.operator;

import flink.model.Message;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class DuplicateFilter extends RichFlatMapFunction<Message, Message> {

    private static final ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class, false);
    private ValueState<Boolean> operatorState;

    @Override
    public void open(Configuration configuration) {
        operatorState = this.getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Message value, Collector<Message> out) throws Exception {
        if (!operatorState.value()) {
            // we haven't seen the element yet
            out.collect(value);
            // set operator state to true so that we don't emit elements with this key again
            operatorState.update(true);
        }
    }
}
