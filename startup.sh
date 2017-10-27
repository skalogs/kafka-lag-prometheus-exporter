#!/bin/bash
set -e

/usr/bin/java -jar /usr/share/prometheus-exporter/kafka-lag-exporter.jar \
  --kafka-host ${KAFKA_HOST} \
  --kafka-port ${KAFKA_PORT} \
  --group-blacklist-regexp ${KAFKA_GROUP_BLACKLIST_REGEXP} \
  --scrape-period ${SCRAPE_PERIOD} \
  --scrape-period-unit ${SCRAPE_PERIOD_UNIT} \
  --port ${PROMETHEUS_PORT}
