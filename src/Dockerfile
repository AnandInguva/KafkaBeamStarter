FROM openjdk:11-jdk-slim


WORKDIR /app

COPY "/Users/anandinguva/Desktop/projects/KafkaProject/target/KafkaProject-1.0-SNAPSHOT-jar-with-dependencies.jar" "/app/app.jar"

ENTRYPOINT ["java", "-jar", "app.jar", "org.example.KafkaClient"]
