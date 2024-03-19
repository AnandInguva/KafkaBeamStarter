package org.example;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;

public class ClientProperties {
    // Add SASL PLAIN TEXT details

    public static ImmutableMap<String, Object> get(){
        final String gmkUsername = System.getenv("GMK_USERNAME");
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, GMKConstants.bootStrapServers);
        properties.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        // Note: in other languages, set sasl.username and sasl.password instead.
        properties.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\'"
                        + gmkUsername
                        + "\'"
                        + " password=\'"
                        + System.getenv("GMK_PASSWORD")
                        + "\';");
        return properties.buildOrThrow();
    }
}
