package org.example;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

final class AdminClient implements AutoCloseable {

    private final Admin admin;
    private static final int numPartitions = 1;
    private static final Integer replicationFactor = 1;

    public AdminClient() {
        admin = Admin.create(ClientProperties.get());
    }

    @Override
    public void close() throws Exception {
        admin.close();
    }

    private static NewTopic defaultTopic(String topicName) {
        return new NewTopic(topicName, numPartitions, replicationFactor.shortValue());
    }

    public void enusreTopicExists(String topicName) throws Exception {
        try {
            admin.createTopics(Collections.singleton(defaultTopic(GMKConstants.topic))).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException){
//                throw e;
                return;
            }
            throw e;
        }
    }
}
