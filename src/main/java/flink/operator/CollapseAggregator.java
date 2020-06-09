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
    public class CollapseAggregator implements AggregateFunction<Message, List<Message>, List<Message>> {

    @Override
    public List<Message> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Message> add(Message inputMessage, List<Message> inputMessages) {
        inputMessages.add(inputMessage);
        return inputMessages.stream().distinct().collect(Collectors.toList());
//        boolean flag = false;
//        for (Message message : inputMessages) {
//            if (message.getSender().equals(inputMessage.getSender())
//                    && message.getRecipient().equals(inputMessage.getRecipient())
//                    && message.getSum() != null
//                    && inputMessage.getSum() != null) {
//                message.setSum(message.getSum() + inputMessage.getSum());
//                flag = true;
//            }
//        }
//        if (!flag) {
//            inputMessages.add(inputMessage);
//        }
//        return inputMessages;
    }

    @Override
    public List<Message> getResult(List<Message> messages) {
        return messages;
    }

    @Override
    public List<Message> merge(List<Message> inputMessages, List<Message> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}
