#!/bin/bash
set -e

# Start Kafka in background (the official image has its own start logic; this varies by image)
# If this image starts Kafka via /etc/kafka/docker/run or similar, you must call the correct one.
# Example placeholder (may differ):
/etc/kafka/docker/run &

echo "Waiting for Kafka..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topic..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic reddit-posts --partitions 1 --replication-factor 1

echo "Kafka running..."
wait
