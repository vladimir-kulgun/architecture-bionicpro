FROM maven:3.9-eclipse-temurin-11 AS provider-build
WORKDIR /build
COPY yandex-provider/pom.xml .
RUN mvn dependency:go-offline -q
COPY yandex-provider/src ./src
RUN mvn package -DskipTests -q

FROM quay.io/keycloak/keycloak:21.1
COPY --from=provider-build /build/target/keycloak-yandex-provider-*.jar /opt/keycloak/providers/
