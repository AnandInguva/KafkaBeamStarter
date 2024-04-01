package org.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.GMKConstants;

public class BeamKafkaConsumer {
    public static class PrintElements<OutputT> extends DoFn<KafkaRecord<String, OutputT>, Void> {
        @ProcessElement
        public void processElement(@Element KafkaRecord<String, OutputT> record) {
            System.out.println(record.getKV());
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        p.apply(
                KafkaIO.<String, String>read()
                        .withBootstrapServers(GMKConstants.bootStrapServers)
                        .withTopic(GMKConstants.topic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
        ).apply(
                ParDo.of(new PrintElements())
        );

        p.run().waitUntilFinish();
    }
}
