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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.FullName;
import org.example.avro.SimpleMessage;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClient {
    private static final Duration sleep = Duration.ofMinutes(1);
    public static class Producer {

        public static KafkaProducer of() {
            Map<String, Object> map = new HashMap<>();
            map.putAll(ClientProperties.get());
//            map.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//            map.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
//            map.put("auto.register.schemas", true);
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

    public static FullName produceRecord(int id) {
        return new FullName("Anand", "Inguva", id);
    }
    public static void main(String[] args) throws  Exception {

//        try (AdminClient adminClient = new AdminClient()) {
//            adminClient.enusreTopicExists(GMKConstants.topic);
//        }

        KafkaProducer producer = Producer.of();
        KafkaConsumer consumer = Consumer.of();


        int msgCount = 0;
        while (true) {
            try {
                FullName name = produceRecord(msgCount);
                SimpleMessage simpleMessage = new SimpleMessage(String.format("Message count: %s", msgCount), String.valueOf(msgCount));
                producer.send(new ProducerRecord(GMKConstants.topic, "message1", name)).get();
                producer.send(new ProducerRecord(GMKConstants.topic, "message2", simpleMessage));
                logger.log(Level.INFO,String.format("Published message %s with id: %s", name, msgCount));
//                logger.log(Level.INFO, Str);
                msgCount++;
                Thread.sleep(1 * 1000 * 10);
                logger.log(Level.INFO, String.format("Received message: %s", name));
            } catch (Throwable e) {
                continue;
            }
        }
    }
}

