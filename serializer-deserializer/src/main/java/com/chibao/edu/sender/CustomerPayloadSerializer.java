package com.chibao.edu.sender;

import com.chibao.edu.events.CustomerPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class CustomerPayloadSerializer  implements Serializer<CustomerPayload> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final class MarshallingException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private MarshallingException(Throwable cause) { super(cause); }
    }

    @Override
    public byte[] serialize(String topic, CustomerPayload payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (JsonProcessingException e) {
            throw new MarshallingException(e);
        }
    }
}
