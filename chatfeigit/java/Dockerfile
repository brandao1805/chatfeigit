# Etapa de build: Maven compila Java e gera as classes do contrato Protobuf.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
# Disponibiliza o schema ao plugin Protobuf do Maven.
COPY proto /build/proto
COPY java /build/java
# O JAR final reúne a aplicação e suas dependências; os testes locais são executados à parte.
RUN mvn -B -f java/pom.xml package -DskipTests
# Runtime menor, sem ferramentas de compilação, para executar o JAR.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/java/target/chatfei-1.0.0.jar /app/chatfei.jar
# O argumento do Compose escolhe server ou bot no método main.
ENTRYPOINT ["java", "-jar", "/app/chatfei.jar"]

