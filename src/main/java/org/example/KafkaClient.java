package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClient {

    static Logger logger = Logger.getLogger("logger");
    public static void main(String[] args) throws  Exception {


        try (AdminClient adminClient = new AdminClient()) {
            adminClient.enusreTopicExists(GMKConstants.topic);
        }

        KafkaProducer producer = new KafkaProducer(ClientProperties.get(), new StringSerializer(), new StringSerializer());

        Map<String, Object> properties = new HashMap<>();
        properties.putAll(ClientProperties.get());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-id");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer consumer = new KafkaConsumer(properties);

        int msgCount = 0;
        while (true) {
            try {
                String message = String.format("message %s", msgCount);
                producer.send(new ProducerRecord(GMKConstants.topic, message)).get();
                logger.log(Level.INFO,"Published message %s", msgCount);
                msgCount++;
                ConsumerRecords<String, String> record = consumer.poll(Duration.ofMinutes(1));
                logger.log(Level.INFO, "Received message: %s", record);
            } catch (Throwable e) {
                continue;
            }
        }
    }
}
