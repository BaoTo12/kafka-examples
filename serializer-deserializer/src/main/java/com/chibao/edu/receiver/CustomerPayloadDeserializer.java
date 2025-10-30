package com.chibao.edu.receiver;

import com.chibao.edu.events.CustomerPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomerPayloadDeserializer implements Deserializer<CustomerPayloadOrError> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CustomerPayloadOrError deserialize(String topic, byte[] data) {
        final var value = new String(data);
        try {
            final var payload = objectMapper.readValue(value, CustomerPayload.class);
            return new CustomerPayloadOrError(payload, null, value);
        } catch (Throwable e) {
            return new CustomerPayloadOrError(null, e, value);
        }
    }
}
