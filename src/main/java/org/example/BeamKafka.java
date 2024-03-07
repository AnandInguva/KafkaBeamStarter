package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.api.client.json.Json;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BeamKafka {

    private static class EmitElements extends DoFn<Long, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element Long element, OutputReceiver<KV<String, String>> receiver) {

            KV<String, String> result = KV.of("Key", "Value");
            receiver.output(result);
        }
    }
    private static class PrintElementFn extends DoFn<String,Void>{
        @ProcessElement
        public void processElement(@Element String input){
            System.out.println(input);
        }
    }
    public static class JsonKafkaMessageFn extends DoFn<Long, String> {

        @ProcessElement
        public void processElement(
                @Element Long element,
                @Timestamp Instant timestamp,
                OutputReceiver<String> receiver,
                ProcessContext context)
                throws IOException {
            receiver.output(
                    String.valueOf(JsonParser.parseString(element.toString())));
        }
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        PeriodicImpulse impulse = PeriodicImpulse.create().withInterval(Duration.millis(500));
        PCollection<String> emitElements = p.apply(impulse).apply(
                MapElements.via(
                        new SimpleFunction<Instant, Long>() {
                            @Override
                            public Long apply(Instant input) {
                                return input.getMillis();
                            }
                        }
                )
        ).apply(ParDo.of(new JsonKafkaMessageFn()));

        emitElements.apply("WriteToKafka",
                KafkaIO.<Void, String>write()
                        .withBootstrapServers("kafkaio-anandinguva-load-test-m:9092")
                        .withTopic("quickstart-events").withValueSerializer(StringSerializer.class).values());

        emitElements.apply(ParDo.of(new PrintElementFn()));

        p.run().waitUntilFinish();
    }
}
