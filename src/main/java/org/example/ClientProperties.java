package org.example;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;

public class ClientProperties {
    // Add SASL PLAIN TEXT details
    private static final String AUTH_TOKEN_USERNAME_PREFIX = "__AUTH_TOKEN__";
//    private static final String GMK_USERNAME = "anandinguva-gmk-test@gmk-creds-testing.iam.gserviceaccount.com";

    private static final String GMK_USERNAME = "anandinguva@dataflow-testing-311516.iam.gserviceaccount.com";
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
                        + AUTH_TOKEN_USERNAME_PREFIX + GMK_USERNAME
                        + "\'"
                        + " password=\'"
                        + "ya29.c.c0AY_VpZjFmQ6kzRV1dfkjbACpS4-qgNr2eKSkhagcNnA6s7bo1PL4urlP6mfCvsFSG4hRwqBbHb4v5BovlDT4TnGPrraawk8EMAbd2e6bA60mZmU9XsPTFKGV4va7cEGFITbVQfKq44Kd5zS-zxCQtWK_BfWJV6KlvwBH9x1j4oyk-LKOyO4b0do832fOybn7jpWaQpJCQEmiunqoX5tc-VqEkpkvUDHaxcCZFXr0vteLWJQqsmzSP-ZIPZNf5lJI0iLvjG25LCajZyCD2wZrhSq0ZmqYMLaE_VpZ1MGo50gKcRqLuOl6T976wMhptA_kgszBTwUMAsgwPnNM-VhNGgSOjZR3xFYMxC0NOOYiT8xky5VO8O2G_LoLg-EJ1eLrYQG395Ak5zBIoIoFjWnIS4U9iQ0r0eO8ZWSiyeWVaIFSJ4nQaq4_i1wRRJr7IpM-oJfkcOWFi8o451WscboIm0FwqR7eV0Ifv8529-nuZeoqruveyBglk1fVe4iVBcm_JuMSYcjMzyuec-hl-mZQIfos4RR_Jzwf3_hxJRWMQveaIxp-5w7t53ftJvFmJ4RmvxdnS8F1fr7O73vuXRpeieJ7I3Wpa__eO1tM4pBgZsQbiIceJFVtcwJ3b2mFffVd1Y2v9dF60b-pq89bY8_w6B55y7ibaiUim9-0J3IiUMxl9gqRi7F86Oxe9k8Oybwh8mjMUqrQyOUSIj0zMYw8YeQjnzxskYUJQZrcYShc6l5zJbJ1fBqjSfXar4cQv21XiipbS-l-khlcbjVXmBvSjgY-5vFt1jM3v9al7uein-3rrixMqujtkBB4n9qmQwnoJlR1Q9fhm1BSnpqF9U0ZSZz12hiFl9escOcF4FJuU09dS3fQ2Sg9Wnhbxth483k067mOq1B5YOersZWV9jVMrIFUkg26yY7OJz3q4VhW7J1b2uv1Zdk1gg7YBeuibSRFtrRqlSiJk4c10_nU1i6x-I89kdfp7FmQShJtqyz2szy7xBqmvI7Wfe2"
                        + "\';");
        return properties.buildOrThrow();
    }
}
