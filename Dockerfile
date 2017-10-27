FROM java:8u111-jdk

ENV KAFKA_HOST kafka
ENV KAFKA_PORT 9092
ENV KAFKA_GROUP_BLACKLIST_REGEXP "console-consumer.*"
ENV SCRAPE_PERIOD 1
ENV SCRAPE_PERIOD_UNIT MINUTES
ENV PROMETHEUS_PORT 7979

ADD startup.sh /usr/bin/startup.sh

CMD ["/usr/bin/startup.sh"]

# Add the service itself
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/prometheus-exporter/kafka-lag-exporter.jar
