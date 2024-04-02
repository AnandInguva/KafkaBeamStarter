#!/bin/sh

source /Users/anandinguva/Desktop/projects/shflags/shflags

# Override the following defaults if you're using internal-client.yaml.
DEFINE_string client_config "client.yaml" "The YAML file containing the client config" c
DEFINE_string namespace "gmk-kafka-client" "The namespace that the client runs in, where the gmk-sasl-plain-login secret should be created. Based on the client config YAML." n

# parse the command-line
FLAGS "$@" || exit $?

# Build image. TODO: Change the tag if you are using a different image.
docker build -t us-docker.pkg.dev/dataflow-testing-311516/df-test-us/anandinguva_kafka_avro_producer:latest

docker push us-docker.pkg.dev/dataflow-testing-311516/df-test-us/anandinguva_kafka_avro_producer:latest

# Switch to the GKE cluster to deploy the kafka client to on GKE.
GKE_CLUSTER=anandinguva-gmk-cluster
PROJECT=dataflow-testing-311516
REGION=us-central1
gcloud config set project $PROJECT
gcloud container clusters get-credentials $GKE_CLUSTER --region=$REGION

kubectl get secret gmk-sasl-plain-login -n "${FLAGS_namespace}"

kubectl apply -f "${FLAGS_client_config}"

# Change the name of the client in the client.yaml as well if you are changing the name here.
kubectl rollout restart deployment anandinguva-kafka-producer -n "${FLAGS_namespace}"

