FROM openjdk:11-jdk-slim


WORKDIR /app

COPY "target/KafkaProject-1.0-SNAPSHOT-jar-with-dependencies.jar" "/app/app.jar"
COPY "src/main/avro/fullName.avsc" "/app/fullName.avsc"
COPY "src/main/avro/simpleMessage.avsc" "/app/simpleMessage.avsc"

ENTRYPOINT ["java", "-jar", "app.jar", "org.example.KafkaClient"]
