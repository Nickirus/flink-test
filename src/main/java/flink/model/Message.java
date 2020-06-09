package flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@JsonSerialize
public class Message {
    private String sender;
    private String recipient;
    private UUID uuid;
    private String message;
    private Integer sum;
    private LocalDateTime sentAt;

    public Message() {
        this.uuid = UUID.randomUUID();
        this.sentAt = LocalDateTime.now();

        //для демонстрации, что работает Watermark в MessageTimestampAssigner для EventTime
//        this.sentAt = LocalDateTime.now().plusSeconds(3);

//        this.sentAt = LocalDateTime.of(2020,12,24,12,0);
    }

    public LocalDateTime getSentAt() {
        return sentAt;
    }

    public void setSentAt(LocalDateTime sentAt) {
        this.sentAt = sentAt;
    }

    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message1 = (Message) o;
        return Objects.equals(sender, message1.sender) &&
                Objects.equals(recipient, message1.recipient) &&
                Objects.equals(uuid, message1.uuid) &&
                Objects.equals(message, message1.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sender, recipient, uuid, message);
    }
}
