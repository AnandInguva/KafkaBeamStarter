package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.FullName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClient {
    private static final Duration sleep = Duration.ofSeconds(10);
    public static class Producer {

        public static KafkaProducer of() {
            Map<String, Object> map = new HashMap<>();
            map.putAll(ClientProperties.get());
            map.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            map.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
            map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            KafkaProducer producer = new KafkaProducer(map);
            return producer;
        }
    }

    public static class Consumer {
        public static KafkaConsumer of() {
            Map<String, Object> properties = new HashMap<>();
            properties.putAll(ClientProperties.get());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-id");
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            KafkaConsumer consumer = new KafkaConsumer(properties);
            return consumer;
        }
    }

    static Logger logger = Logger.getLogger("logger");

    public static FullName produceRecord() {
        return new FullName("Anand", "Inguva");
    }
    public static void main(String[] args) throws  Exception {

        try (AdminClient adminClient = new AdminClient()) {
            adminClient.enusreTopicExists(GMKConstants.topic);
        }

        KafkaProducer producer = Producer.of();
        KafkaConsumer consumer = Consumer.of();

        int msgCount = 0;
        while (true) {
            try {
                FullName name = produceRecord();
                producer.send(new ProducerRecord(GMKConstants.topic, "message1", name)).get();
                logger.log(Level.INFO,"Published message %s", msgCount);
                msgCount++;
                ConsumerRecords<String, String> record = consumer.poll(Duration.ofMinutes(1));
                logger.log(Level.INFO, "Received message: %s", record);
                Thread.sleep(sleep.toMillis());
            } catch (Throwable e) {
                continue;
            }
        }
    }
}
