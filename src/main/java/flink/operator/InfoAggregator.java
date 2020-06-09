package flink.operator;

import flink.model.Message;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class InfoAggregator implements AggregateFunction<Message, List<Message>, String> {

    @Override
    public List<Message> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Message> add(Message inputMessage, List<Message> inputMessages) {
        inputMessages.add(inputMessage);
        return inputMessages;
    }

    @Override
    public String getResult(List<Message> inputMessages) {
        Integer sum = 0;
        for (Message message : inputMessages) {
            if (message.getSum() != null)
            sum += message.getSum();
        }
        return String.valueOf(LocalDateTime.now()).concat(
                String.format(" - During the last 15 seconds the user-%s sent %s messages. Total amount is %s - %s",
                        inputMessages.get(0).getSender(),
                        inputMessages.size(),
                        sum, inputMessages.get(0).getUuid()));
    }

    @Override
    public List<Message> merge(List<Message> inputMessages, List<Message> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}