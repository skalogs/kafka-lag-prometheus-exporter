package io.adopteunops.monitoring.kafka.exporter;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConsumerGroupMetrics {

    ConsumerGroupMetrics() {
    }

    private List<PartitionMetric> metrics = new ArrayList<>();

    void add(TopicPartition topicPartition, MetricType type, Long value) {
        metrics.add(new PartitionMetric(topicPartition, type, value));
    }

    Map<String,Long> getTotalLagPerTopic() {
        return metrics.stream().filter(metric -> metric.type == MetricType.LAG)
                .collect(Collectors.groupingBy(metric -> metric.topicPartition.topic(),
                        Collectors.summingLong(metrics -> metrics.value)));
    }

    Map<String,Long> getCurrentOffsetSumPerTopic() {
        return metrics.stream().filter(metric -> metric.type == MetricType.CURRENT_OFFSET)
                .collect(Collectors.groupingBy(metric -> metric.topicPartition.topic(),
                        Collectors.summingLong(metrics -> metrics.value)));
    }

    private static class PartitionMetric {
        TopicPartition topicPartition;
        MetricType type;
        Long value;

        PartitionMetric(TopicPartition topicPartition, MetricType type, Long value) {
            this.topicPartition = topicPartition;
            this.type = type;
            this.value = value;
        }
    }
}
