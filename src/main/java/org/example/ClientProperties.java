package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;

public class ClientProperties {
    // Add SASL PLAIN TEXT details
    // Append the suffix to the GMK_USERNAME if we are using auth tokens.
    private static final String AUTH_TOKEN_USERNAME_PREFIX = "__AUTH_TOKEN__";
    private static final String GMK_USERNAME = "anandinguva-gmk-test@gmk-creds-testing.iam.gserviceaccount.com";
    public static ImmutableMap<String, Object> get(){

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, GMKConstants.bootStrapServers);
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//         Note: in other languages, set sasl.username and sasl.password instead.
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\'"
                        +  GMK_USERNAME
                        + "\'"
                        + " password=\'"
                        + System.getenv("GMK_PASSWORD")
                        + "\';");
        return properties.buildOrThrow();
    }
}
