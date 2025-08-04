package org.coding.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am a Kafka Producer");
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // tell the producer to send all data and block until done
        producer.flush();

        // flush and close the producer
        producer.close();
    }

    // kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3
    // kafka-topics.sh --bootstrap-server localhost:9092 --list
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java
}
