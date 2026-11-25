FROM maven:3.9.6-eclipse-temurin-17-alpine AS builder
WORKDIR /app
COPY . .
# Compile the project
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/vault-node/target/vault-node-*.jar app.jar
# Default entrypoint: read mounted secret file only when DB_PASSWORD is unset; never fall back to a default password
ENTRYPOINT ["sh", "-c", "if [ -z \"${DB_PASSWORD:-}\" ] && [ -r /run/secrets/db_password ]; then export DB_PASSWORD=\"$(tr -d '\\r\\n' < /run/secrets/db_password)\"; fi; exec java -Xmx512m -jar app.jar"]
