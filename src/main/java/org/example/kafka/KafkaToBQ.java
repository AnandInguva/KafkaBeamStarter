package org.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.GenericRecordCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
//import com.google.cloud.teleport.v2.transforms.BigQueryConverters;



import java.util.*;

public class KafkaToBQ {
    private static class PrintRecord extends DoFn<KV<byte[], GenericRecord>, Void> {
        @ProcessElement
        public void processElement(@Element KV<byte[], GenericRecord> record) {
            System.out.println(record);
        }
    }
    public static class DeserializeBytes extends DoFn<KV<byte[], byte[]>, KV<byte[], GenericRecord>> {
        @ProcessElement
        public void processElement(@Element KV<byte[], byte[]> data, OutputReceiver<KV<byte[], GenericRecord>> output) {
            KafkaAvroDeserializer deserializer  = new KafkaAvroDeserializer();
            Map<String, String> kafkaProps = new HashMap<>();
            kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            kafkaProps.put("specific.avro.reader", "true");
            deserializer.configure(kafkaProps, true);
            byte[] byteValue = data.getValue();
            final GenericRecord o = (GenericRecord) deserializer.deserialize("quickstart-event", byteValue);
            final byte[] key = data.getKey();
            output.output(KV.of(key, o));
        }
    }

    public static PTransform<PBegin, PCollection<KV<byte[], byte[]>>> readAvroFromKafka(
            String bootstrapServers,
            List<String> topicsList,
            Map<String, Object> config
    ) {
        KafkaIO.Read<byte[], byte[]> kafkaRecords = KafkaIO.<byte[], byte[]>read()
                        .withBootstrapServers(bootstrapServers)
                        .withKeyDeserializerAndCoder(
                                ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withTopics(topicsList);
        return kafkaRecords.withoutMetadata();
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String topic = "quickstart-event";
        String subject = "fullName";
        String bootstrapServer = "http://localhost:9092";

        PCollection<KV<byte[], GenericRecord>> genericRecords = pipeline.apply(
                readAvroFromKafka(bootstrapServer, Collections.singletonList(topic), null)
        ).apply(ParDo.of(new DeserializeBytes())).setCoder(KvCoder.of(ByteArrayCoder.of(), GenericRecordCoder.of()));

//
        genericRecords.apply(ParDo.of(new PrintRecord()));


        /*
        * Step #2: Transform the Kafka messages into TableRows.
         */
        PCollectionTuple convertedTableRows;
        WriteResult writeResult;

//     s.create()).apply(Convert.toRows()).apply(BigQueryConverters.<Row>createWriteTransform(options).useBeamSchema());


        pipeline.run().waitUntilFinish();
    }
}
