package org.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.ClientProperties;
import org.example.GMKConstants;
//import com.google.cloud.teleport.v2.transforms.BigQueryConverters;



import java.util.*;

public class KafkaToBQ {
    public static class DeserializeBytes extends DoFn<KV<byte[], byte[]>, KV<byte[], GenericRecord>> {
        @ProcessElement
        public void processElement(@Element KV<byte[], byte[]> data, OutputReceiver<KV<byte[], GenericRecord>> output) {
            KafkaAvroDeserializer deserializer  = new KafkaAvroDeserializer();
            Map<String, String> kafkaProps = new HashMap<>();
            deserializer.configure(kafkaProps, true);
            byte[] byteValue = data.getValue();
            final GenericRecord o = (GenericRecord) deserializer.deserialize("quickstart-event", byteValue);
            final byte[] key = data.getKey();
            output.output(KV.of(key, o));
        }
    }

    public static class PrintElements extends DoFn<KafkaRecord<String, String>, Void> {

        @ProcessElement
        public void processElement(@Element KafkaRecord<String, String> record) {
            System.out.println(record.getKV());
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
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        String topic = GMKConstants.topic;
        String bootstrapServer = GMKConstants.bootStrapServers;
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < GMKConstants.partitions; i++) {
            partitions.add(new TopicPartition(topic, i));
        }


       PCollection<KafkaRecord<String, String>> kafkaRead = pipeline.apply(
               KafkaIO.<String, String>read()
                       .withBootstrapServers(bootstrapServer)
                       .withTopicPartitions(partitions)
                       .withKeyDeserializerAndCoder(
                       StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                        .withValueDeserializerAndCoder(
                       StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                       .withConsumerConfigUpdates(ClientProperties.get())
       );
       kafkaRead.apply(ParDo.of(new PrintElements()));

       pipeline.run().waitUntilFinish();
    }
}
