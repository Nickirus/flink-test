package flink.operator;

import flink.model.Message;
import org.apache.flink.api.common.functions.MapFunction;

public class WordsMapper implements MapFunction<Message, String> {

    @Override
    public String map(Message s) {
        return s.getUuid()+ " - " + "\"" + s.getMessage() + "\"" + " from " + s.getSender() + " to " + s.getRecipient();
    }
}
