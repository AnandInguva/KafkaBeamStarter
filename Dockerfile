FROM openjdk:11-jdk-slim


WORKDIR /app

COPY "target/KafkaProject-1.0-SNAPSHOT-jar-with-dependencies.jar" "/app/app.jar"

ENTRYPOINT ["java", "-jar", "app.jar", "org.example.KafkaClient"]
