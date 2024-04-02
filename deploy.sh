#!/bin/bash

# Override the following defaults if you're using internal-client.yaml.
DEFINE_string client_config "client.yaml" "The YAML file containing the client config"
DEFINE_string namespace "gmk-kafka-client" "The namespace that the client runs in, where the gmk-sasl-plain-login secret should be created. Based on the client config YAML."

# Build image. TODO: Change the tag if you are using a different image.
docker build -t us-docker.pkg.dev/dataflow-testing-311516/df-test-us/anandinguva_kafka_avro_producer:latest

docker push us-docker.pkg.dev/dataflow-testing-311516/df-test-us/anandinguva_kafka_avro_producer:latest


kubectl get secret gmk-sasl-plain-login -n ${FLAGS_namespace}

kubectl apply -f "${FLAGS_client_config}"

# Change the name of the client in the client.yaml as well if you are changing the name here.
kubectl rollout restart deployment anandinguva-kafka-producer -n ${FLAGS_namespace}

