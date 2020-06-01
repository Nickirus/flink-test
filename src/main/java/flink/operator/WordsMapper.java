package flink.operator;

import org.apache.flink.api.common.functions.MapFunction;

public class WordsMapper implements MapFunction<String, String> {

    @Override
    public String map(String s) {
        return String.valueOf(s.length());
    }
}
