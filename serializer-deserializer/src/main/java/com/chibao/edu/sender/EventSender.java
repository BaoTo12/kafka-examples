package com.chibao.edu.sender;

import com.chibao.edu.events.CustomerPayload;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface EventSender extends Closeable {
    Future<RecordMetadata> send(CustomerPayload payload);

    default RecordMetadata blockingSend(CustomerPayload payload) throws SendException, InterruptedException {
        try {
            return send(payload).get();
        } catch (ExecutionException ex) {
            throw new SendException(ex.getCause());
        }
    }

    final class SendException extends Exception {
        private static final long serialVersionUID = 1L;

        SendException(Throwable cause) {
            super(cause);
        }
    }

    @Override
    public void close();
}
