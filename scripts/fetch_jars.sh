#!/usr/bin/env bash
# Download the Spark runtime jars StreamLake needs into a directory, so a container start does
# not depend on Maven Central being reachable. Versions are kept in step with conf/streamlake.yml.
set -euo pipefail

TARGET="${1:-jars}"
MAVEN="${MAVEN_REPO:-https://repo1.maven.org/maven2}"

ICEBERG_VERSION="1.11.0"
SPARK_VERSION="4.0.4"
SCALA="2.13"

ARTIFACTS=(
  "org/apache/iceberg/iceberg-spark-runtime-4.0_${SCALA}/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_${SCALA}-${ICEBERG_VERSION}.jar"
  "org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"
  # These two are transitive dependencies of the Kafka connector. Their versions are not
  # cosmetic: commons-pool2 must be the version the connector was compiled against (2.12.0 adds
  # PoolConfig.setMinEvictableIdleDuration, which the connector calls). Pinning 2.11.1 here made
  # the local run work — Ivy resolved the right one — while every container crashed with
  # NoSuchMethodError on the first micro-batch.
  "org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar"
  "org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"
)

mkdir -p "$TARGET"
for artifact in "${ARTIFACTS[@]}"; do
  name="$(basename "$artifact")"
  if [ -f "$TARGET/$name" ]; then
    echo "have  $name"
    continue
  fi
  echo "fetch $name"
  curl -fsSL "$MAVEN/$artifact" -o "$TARGET/$name"
done

echo "jars in $TARGET:"
ls -1 "$TARGET"
