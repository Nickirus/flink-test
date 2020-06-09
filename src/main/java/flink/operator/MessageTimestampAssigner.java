package flink.operator;

import flink.model.Message;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.ZoneId;

public class MessageTimestampAssigner implements AssignerWithPeriodicWatermarks<Message> {

    @Override
    public long extractTimestamp(Message element, long previousElementTimestamp) {
        ZoneId zoneId = ZoneId.systemDefault();
        return element.getSentAt().atZone(zoneId).toEpochSecond() * 1000;
    }

    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}
