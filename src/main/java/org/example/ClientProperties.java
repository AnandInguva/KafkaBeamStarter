package org.example;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.HashMap;

public class ClientProperties {
    // Add SASL PLAIN TEXT details
    public static ImmutableMap<String, Object> get(){
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, GMKConstants.bootStrapServers);
        properties.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
        return properties.buildOrThrow();
    }
}
