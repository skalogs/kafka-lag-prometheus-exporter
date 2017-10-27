#!/bin/bash
set -e

@Parameter(names = "--kafka-host", description = "Kafka hostname", required = true)
public String kafkaHostname;

@Parameter(names = "--kafka-port", description = "Kafka port")
public int kafkaPort = 9092;

@Parameter(names = "--port", description = "Exporter port")
public int port = 7979;

@Parameter(names = "--group-blacklist-regexp", description = "Consumer group blacklist regexp")
public String groupBlacklistRegexp = "console-consumer.*";

@Parameter(names = "--scrape-period", description = "Scrape period")
public int scrapePeriod = 30;

@Parameter(names = "--scrape-period-unit", description = "Scrape period timeunit")
public TimeUnit scrapePeriodUnit = TimeUnit.SECONDS;



/usr/bin/java -jar /usr/share/prometheus-exporter/kafka-lag-exporter.jar \
  --kafka-host ${KAFKA_HOSTS} \
  --kafka-port ${kafka_PORT} \
  --group-blacklist-regexp ${KAFKA_GROUP_BLACKLIST_REGEXP} \
  --scrape-period ${SCRAPE_PERIOD} \
  --scrape-period-unit ${SCRAPE_PERIOD_UNIT} \
  --port ${PROMETHEUS_PORT}
