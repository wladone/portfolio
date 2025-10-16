# Multi-stage build for log analytics pipeline
# Stage 1: Build the application JAR
FROM openjdk:17-jdk-slim as builder

# Install SBT
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    curl -fsL https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz | tar xfz - -C /opt/ && \
    mv /opt/sbt-* /opt/sbt && \
    ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy SBT configuration first for better caching
COPY project/plugins.sbt project/
COPY build.sbt .scalafmt.conf ./

# Download dependencies
RUN sbt update

# Copy source code
COPY src/ src/

# Build the fat JAR
RUN sbt clean assembly

# Stage 2: Runtime image
FROM openjdk:17-jdk-slim

# Set working directory
WORKDIR /app

# Install required packages for runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        procps \
        netcat \
        && rm -rf /var/lib/apt/lists/*

# Copy the built JAR from builder stage
COPY --from=builder /app/target/scala-2.12/log-analytics-pipeline-1.0.0.jar /app/log-analytics-pipeline.jar

# Copy scripts for convenience
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.sh

# Create application directories
RUN mkdir -p /app/input /app/output /app/checkpoint

# Set environment variables
ENV SPARK_MASTER=local[*]
ENV LOG_LEVEL=WARN
ENV JAVA_OPTS="-Xms1g -Xmx2g"

# Expose ports for Spark UI and other services
EXPOSE 4040 8080 8081 9092 9042

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:4040 || exit 1

# Default command - can be overridden to run different apps
CMD ["java", "-cp", "/app/log-analytics-pipeline.jar", "com.example.logstream.FileStreamApp", "--help"]