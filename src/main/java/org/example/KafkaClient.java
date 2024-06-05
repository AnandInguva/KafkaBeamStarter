package org.example;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;

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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaClient {
    private static final Duration sleep = Duration.ofMinutes(1);
    private static final String fullNameAvroSchemaPath = "/app/fullName.avsc";
    private static final String simpleMessageAvroSchemaPath = "/app/simpleMessage.avsc";
    // Comment these while running on local.
//    private static final String fullNameAvroSchemaPath = "/app/fullName.avsc";
//    private static final String simpleMessageAvroSchemaPath = "/app/simpleMessage.avsc";
    // Decouple Producer from this class to a different class for better readability.
    public static class Producer {

//        public static KafkaProducer of() {
//            StringSerializer keySerializer = new StringSerializer();
//            StringSerializer valueSerializer = new StringSerializer();
//            Map<String, Object> configs = new HashMap<String, Object>();
//            configs.put(
//                    "bootstrap.servers", GMKConstants.bootStrapServers);
//
//            configs.putAll(ClientProperties.get());
//
//            return new KafkaProducer<>(
//                    configs,
//                    keySerializer,
//                    valueSerializer);
//        }

        public static KafkaProducer of(Schema avroSchema) throws RestClientException, IOException {
//            map.putAll(ClientProperties.get());

            // Key serializer
            StringSerializer keySerializer = new StringSerializer();

            // Value serializer using a MockSchemaRegistryClient
            MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
            mockSchemaRegistryClient.register(GMKConstants.topic + "-value", avroSchema, 1, 1);
            KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer(mockSchemaRegistryClient);

            logger.log(Level.INFO,
                    "Created mock schema registry client");

            Map<String, Object> configs = new HashMap<String, Object>();
            configs.put(
                    "bootstrap.servers", GMKConstants.bootStrapServers);

            configs.putAll(ClientProperties.get());

            return new KafkaProducer<>(
                    configs,
                    keySerializer,
                    valueSerializer);
        }

        public static KafkaProducer of() throws RestClientException, IOException {

        //     String schemaRegistryURL = "http://10.128.0.50:8081";
        Map<String, Object> configs = new HashMap<String, Object>();
            configs.put(
                    "bootstrap.servers", GMKConstants.bootStrapServers);
            // Auto register the schema.
            configs.put(
                    KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true
            );
            String schemaRegistryURL = "http://10.196.0.42:8081";    
            configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // Record name strategy for multiple schema topic
        //     configs.put("value.subject.name.strategy", RecordNameStrategy.class.getName());

            configs.putAll(ClientProperties.get());
            return new KafkaProducer<>(
                    configs);
        }
    }

    static Logger logger = Logger.getLogger(KafkaClient.class.getName());

    private static final GenericRecord produceFullName(Schema schema, int id) {
        return new GenericRecordBuilder(schema)
                .set("first", "Fake")
                .set("last", "Message")
                .set("id", id)
                .build();
    }

    private static final GenericRecord produceSimpleMessage(Schema schema, int id) {
        return new GenericRecordBuilder(schema)
                .set("message", "Message with id " + id)
                .set("id", id)
                .build();
    }

    public static void main(String[] args) throws  Exception {

        try (AdminClient adminClient = new AdminClient()) {
            adminClient.enusreTopicExists(GMKConstants.topic);
        }
// ****************************************************************************************************** //


        // Add support for multiple schemas.
        Schema fullNameAvroSchema = new Schema.Parser().parse(new File(fullNameAvroSchemaPath));
        Schema simpleMessageAvroSchema = new Schema.Parser().parse(new File(simpleMessageAvroSchemaPath));


//         Publish Avro messages.
        KafkaProducer producer = Producer.of();



        int msgCount = 0;
        while (true) {
            GenericRecord fullNameRecord = produceFullName(fullNameAvroSchema, msgCount);
            GenericRecord simpleMessageRecord = produceSimpleMessage(simpleMessageAvroSchema, msgCount);

            producer.send(new ProducerRecord(GMKConstants.topic, fullNameRecord)).get();
            logger.log(Level.INFO, String.format("Published message %s with id: %s", fullNameRecord, msgCount));

        //     producer.send(new ProducerRecord(GMKConstants.topic, simpleMessageRecord)).get();
        //     logger.log(Level.INFO, String.format("Published message %s with id: %s", simpleMessageRecord, msgCount));

            msgCount++;
            Thread.sleep(1 * 1000 * 10);
        }
        // ****************************************************************************************************** //

        // Publish Json messages.
//        KafkaProducer producer = Producer.of();
//        int i = 0;
//        while (true) {
//            String message1 = "{\"id\": " + i + ", \"name\": \"Dataflow\"}";
//            String message2 = "{\"id\": " + i + ", \"name\": \"Pub/Sub\"}";
//
//            producer.send(new ProducerRecord(GMKConstants.topic, message1));
//            logger.log(Level.INFO, message1);
//            producer.send(new ProducerRecord(GMKConstants.topic, message2));
//            logger.log(Level.INFO, message2);
//
//            i++;
//            Thread.sleep(1000 * 10);

        // ****************************************************************************************************** /
    }
}

