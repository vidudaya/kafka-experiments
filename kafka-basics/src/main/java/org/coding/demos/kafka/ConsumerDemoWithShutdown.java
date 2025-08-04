package org.coding.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest"); // Start reading from the beginning of the partition (oldest messages)
        // properties.setProperty("auto.offset.reset", "latest"); // Start reading from the end (only new messages going forward).
        // properties.setProperty("auto.offset.reset", "none"); // Throw an exception if no offset is found. (You expect that offsets should always be present, and their absence indicates a bug or misconfiguration.)

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        final Thread mainThread = Thread.currentThread();
        // A shutdown hook is a thread that runs when the application is terminating, such as:
        //Pressing Ctrl+C
        //Calling System.exit(...)
        //Application crash or SIGTERM signal (e.g., Docker stop)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // This is Kafka's safe way to interrupt a blocked poll()
                // Without it, the poll(Duration) call might block indefinitely, and the shutdown could hang.
                // wakeup() throws a WakeupException in the polling thread, which you must catch and handle.

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                logger.info("Polling ...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            logger.error("Unexpected exception in the consumer...");
        } finally {
            consumer.close();
            logger.info("Consumer is now gracefully shutdown...");
        }

    }

    // kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3
    // kafka-topics.sh --bootstrap-server localhost:9092 --list
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java
}
