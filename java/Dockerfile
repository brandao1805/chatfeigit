FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY proto /build/proto
COPY java /build/java
RUN mvn -B -f java/pom.xml package -DskipTests
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/java/target/chatfei-1.0.0.jar /app/chatfei.jar
ENTRYPOINT ["java", "-jar", "/app/chatfei.jar"]

