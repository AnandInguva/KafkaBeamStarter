package org.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.GMKConstants;
import org.example.avro.FullName;
import org.example.avro.SimpleMessage;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaMessageConsumer {
    public static void main(String[] args) {

        // Configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GMKConstants.bootStrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group"); // Identify your consumer group
        //Set value for new property
        props.setProperty("value.subject.name.strategy", RecordNameStrategy.class.getName());
        String topic = GMKConstants.topic;
        KafkaConsumer<String, SpecificRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        Logger logger = Logger.getLogger("logger");


        try {
            while (true) {
                ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, SpecificRecord> record : records) {
                    String message = String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    logger.log(Level.INFO, message);
                }
            }
        } finally {
            consumer.close(); // Close the consumer on exit
        }

    }
}
