package flink.operator;

import flink.model.Message;
import flink.model.Snapshot;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * При наличие во временном окне еще одной такого же сообщения,
 * но с другой суммой, нужно схлопнуть это сообщение в одно и сумму сложить
 */
public class CollapseAggregator implements AggregateFunction<Message, List<Message>, Snapshot> {

    @Override
    public List<Message> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Message> add(Message newMessage, List<Message> messages) {
        boolean flag = false;
        for (Message message : messages) {
            if (message.getSender().equals(newMessage.getSender())
                    || message.getRecipient().equals(newMessage.getRecipient())) {
                message.setSum(message.getSum() + newMessage.getSum());
                flag = true;
                break;
            }
        }
        if (!flag) {
            messages.add(newMessage);
        }
        return messages;
    }

    @Override
    public Snapshot getResult(List<Message> messages) {
        return new Snapshot(messages, LocalDateTime.now());
    }

    @Override
    public List<Message> merge(List<Message> inputMessages, List<Message> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}
