package com.chibao.edu.receiver;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Map;

public class RunDirectConsumer {
    public static void main(String[] args) throws InterruptedException{
        final var consumerConfig =
                Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, "customer-direct-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try(var receiver = new DirectReceiver(consumerConfig, "customer.test", Duration.ofMillis(1000))){
            new ConsumerBusinessLogic(receiver);
            receiver.start();
            Thread.sleep(10_000);
        }
    }
}
