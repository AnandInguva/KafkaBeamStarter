package org.example;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;

public class SchemaUtils {

    static final String schemaString = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"org.example.avro\",\n" +
            "  \"name\": \"FullName\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"first\", \"type\": \"string\" },\n" +
            "    { \"name\": \"last\", \"type\": \"string\" },\n" +
            "    { \"name\" :  \"id\", \"type\":  \"int\", \"default\": 0}\n" +
            "  ]\n" +
            "}";
    public static final Schema avroSchema = new Schema.Parser().parse(schemaString);

    public static final MockSchemaRegistryClient mockSchemaRegistry = new MockSchemaRegistryClient();


    public static void register(Schema schema, int version, int schemaID) {


    }
}
