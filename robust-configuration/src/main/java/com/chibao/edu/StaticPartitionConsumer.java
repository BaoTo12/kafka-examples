package com.chibao.edu;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class StaticPartitionConsumer {
    public static void main(String[] args) {
        // Set up Kafka consumer configuration
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "static-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Set up Kafka consumer with statically assigned partitions
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition0 = new TopicPartition("my-topic", 0);
        TopicPartition partition1 = new TopicPartition("my-topic", 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        // Start consuming messages from assigned partitions
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofDays(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}