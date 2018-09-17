package io.adopteunops.monitoring.kafka.exporter;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerGroupMetricsTest {

    @Test
    public void testMetrics() {
        TopicPartition topicPartition11 = new TopicPartition("topic1", 1);
        TopicPartition topicPartition12 = new TopicPartition("topic1", 2);
        TopicPartition topicPartition13 = new TopicPartition("topic1", 3);
        TopicPartition topicPartition21 = new TopicPartition("topic2", 1);
        TopicPartition topicPartition22 = new TopicPartition("topic2", 2);

        ConsumerGroupMetrics metrics = new ConsumerGroupMetrics();
        metrics.add(topicPartition11, MetricType.LAG, 11L);
        metrics.add(topicPartition12, MetricType.LAG, 22L);
        metrics.add(topicPartition13, MetricType.LAG, 33L);
        metrics.add(topicPartition21, MetricType.LAG, 44L);
        metrics.add(topicPartition22, MetricType.LAG, 55L);

        metrics.add(topicPartition11, MetricType.CURRENT_OFFSET, 1L);
        metrics.add(topicPartition12, MetricType.CURRENT_OFFSET, 2L);
        metrics.add(topicPartition13, MetricType.CURRENT_OFFSET, 3L);
        metrics.add(topicPartition21, MetricType.CURRENT_OFFSET, 4L);

        Assert.assertEquals(new Long(11L + 22 + 33), metrics.getTotalLagPerTopic().get("topic1"));
        Assert.assertEquals(new Long(44L + 55), metrics.getTotalLagPerTopic().get("topic2"));
        Assert.assertEquals(new Long(1L + 2 + 3), metrics.getCurrentOffsetSumPerTopic().get("topic1"));
        Assert.assertEquals(new Long(4L), metrics.getCurrentOffsetSumPerTopic().get("topic2"));
    }

}
