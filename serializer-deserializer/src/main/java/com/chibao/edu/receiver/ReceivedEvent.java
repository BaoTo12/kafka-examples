package com.chibao.edu.receiver;

import com.chibao.edu.events.CustomerPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReceivedEvent {
    private final CustomerPayload payload;
    private final Throwable error;
    private final ConsumerRecord<String, ?> record;
    private final String encodedValue;
    public ReceivedEvent(CustomerPayload payload, Throwable error, ConsumerRecord<String, ?> record, String encodedValue) {
        this.record = record;
        this.payload = payload;
        this.error = error;
        this.encodedValue = encodedValue;
    }

    public CustomerPayload getPayload() {
        return payload;
    }

    public boolean isError() {
        return error != null;
    }

    public Throwable getError() {
        return error;
    }

    public ConsumerRecord<String, ?> getRecord() {
        return record;
    }

    public String getEncodedValue() {
        return encodedValue;
    }

    @Override
    public String toString() {
        return ReceivedEvent.class.getSimpleName() + " [payload=" + payload + ", error=" + error +
                ", record=" + record + ", encodedValue=" + encodedValue + "]";
    }
}
