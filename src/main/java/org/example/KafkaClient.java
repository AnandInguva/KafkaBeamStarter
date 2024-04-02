package org.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.FullName;
import org.example.avro.SimpleMessage;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClient {
    private static final Duration sleep = Duration.ofMinutes(1);
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
    public static class Producer {

        public static KafkaProducer of(Schema avroSchema) throws RestClientException, IOException {
//            map.putAll(ClientProperties.get());

            // Key serializer
            StringSerializer keySerializer = new StringSerializer();

            // Value serializer using a MockSchemaRegistryClient

            // We need to pass stream instead of string for a file. So, for now I am directly
            // pasting the value of the schema here.

            MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
            mockSchemaRegistryClient.register(GMKConstants.topic + "-value", avroSchema, 1, 1);
            KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer(mockSchemaRegistryClient);

            LOG.info("Created mock schema registry client");

            Map<String, Object> configs = new HashMap<String, Object>();
            configs.put(
                    "bootstrap.servers", GMKConstants.bootStrapServers);

            configs.putAll(ClientProperties.get());

            return new KafkaProducer<>(
                    configs,
                    keySerializer,
                    valueSerializer);
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

    static Logger logger = Logger.getLogger(KafkaClient.class.getName());

    private static final GenericRecord produceRecord(Schema schema, int id) {
        return new GenericRecordBuilder(schema)
                .set("first", "Anand")
                .set("last", "Inguva")
                .set("id", id)
                .build();
    }

    public static FullName produceRecord(int id) {
        return new FullName("Anand", "Inguva", id);
    }
    public static void main(String[] args) throws  Exception {

        try (AdminClient adminClient = new AdminClient()) {
            adminClient.enusreTopicExists(GMKConstants.topic);
        }

        String avroSchemaPath = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"org.example.avro\",\n" +
                "  \"name\": \"FullName\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"first\", \"type\": \"string\" },\n" +
                "    { \"name\": \"last\", \"type\": \"string\" },\n" +
                "    { \"name\" :  \"id\", \"type\":  \"int\", \"default\": 0}\n" +
                "  ]\n" +
                "}";
        Schema avroSchema = new Schema.Parser().parse(avroSchemaPath);
        KafkaProducer producer = Producer.of(avroSchema);
        KafkaConsumer consumer = Consumer.of();


        int msgCount = 0;
        while (true) {
            GenericRecord record = produceRecord(avroSchema, msgCount);
            producer.send(new ProducerRecord(GMKConstants.topic, record)).get();
            logger.log(Level.INFO, String.format("Published message %s with id: %s", record, msgCount));
            msgCount++;
            Thread.sleep(1 * 1000 * 10);
        }
    }
}

