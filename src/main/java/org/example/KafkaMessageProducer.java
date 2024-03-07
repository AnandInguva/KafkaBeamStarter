package org.example;

import java.io.File;
import java.util.Properties;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.example.avro.FullName;
import org.example.avro.SimpleMessage;

public class KafkaMessageProducer {
    public static void main(String[] args) {

        // Configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        //Set value for new property
        props.setProperty("value.subject.name.strategy", RecordNameStrategy.class.getName());
        String topic = "quickstart-event";

        SimpleMessage message = new SimpleMessage();
        message.setContent("Hello world");
        message.setDateTime("Fake value");

        FullName name = new FullName();
        name.setFirst("Anand");
        name.setLast("Inguva");

        // Create a producer object
        try (KafkaProducer<String, SpecificRecord> producer = new KafkaProducer<>(props)) {
            // Produce messages
            for (int i = 0; i < 10000; i++) {
                try {
                    Thread.sleep(10 * 30);
                } catch (InterruptedException e) {
                    System.err.format("InterruptedException : %s%n", e);
                }
                producer.send(new ProducerRecord<>(topic, "message1", message));
                producer.send(new ProducerRecord<>(topic, "message2", name));

            }
        }
    }
}