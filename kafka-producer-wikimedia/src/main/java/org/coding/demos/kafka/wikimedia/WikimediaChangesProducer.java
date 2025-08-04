package org.coding.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * In terminal, Run kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange --from-beginning
 * the consumer will wait
 * Then run the main class to produce the data for the consumer to consume, we should see the logs in console as well as consumer output in terminal as well
 */
public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";
        // In localhost use below to create the topic
        // kafka-topics.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange --create --partitions 3

        WikimediaChangeHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder besBuilder = new BackgroundEventSource.Builder(eventHandler, builder);
        try (BackgroundEventSource bes = besBuilder.build()) {
            bes.start(); // start a background thread
            //  if we don’t pause the main thread, the main() method would finish immediately after calling bes.start(),
            //  and the JVM would exit — killing the background thread too early.
            TimeUnit.MINUTES.sleep(10);

            // When this block ends, bes.close() is called automatically
        }
    }

}
