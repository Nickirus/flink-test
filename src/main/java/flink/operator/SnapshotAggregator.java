package flink.operator;

import flink.model.Message;
import flink.model.Snapshot;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class SnapshotAggregator implements AggregateFunction<Message, List<Message>, Snapshot> {

    @Override
    public List<Message> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Message> add(Message message, List<Message> messages) {
        messages.add(message);
        return messages;
    }

    @Override
    public Snapshot getResult(List<Message> messages) {
        return new Snapshot(messages, LocalDateTime.now());
    }

    @Override
    public List<Message> merge(List<Message> messages, List<Message> acc1) {
        messages.addAll(acc1);
        return messages;
    }
}