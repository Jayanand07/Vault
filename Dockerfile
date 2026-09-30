FROM maven:3.9.6-eclipse-temurin-17-alpine AS builder
WORKDIR /app
COPY . .
# Compile the project
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/vault-node/target/vault-node-*.jar app.jar
# Default entrypoint
ENTRYPOINT ["sh", "-c", "export DB_PASSWORD=$(cat /run/secrets/db_password 2>/dev/null || echo vault_secret) && java -Xmx512m -jar app.jar"]
