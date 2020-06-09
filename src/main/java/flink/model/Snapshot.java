package flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class Snapshot {

    @JsonProperty("messages")
    private List<Message> messages;
    @JsonProperty("snapshotTimestamp")
    private LocalDateTime snapshotTimestamp;
    @JsonProperty("uuid")
    private UUID uuid;

    public Snapshot(List<Message> messages, LocalDateTime snapshotTimestamp) {
        this.messages = messages;
        this.snapshotTimestamp = snapshotTimestamp;
        this.uuid = UUID.randomUUID();
    }
}
