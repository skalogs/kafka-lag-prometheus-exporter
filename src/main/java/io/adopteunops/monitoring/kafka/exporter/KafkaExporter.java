/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.adopteunops.monitoring.kafka.exporter;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaExporter {

    private final CollectorRegistry registry;
    private static Gauge gaugeOffsetLag;
    private static Gauge gaugeCurrentOffset;

    private final AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;

    public KafkaExporter(String kafkaHostname, int kafkaPort) {
        this.registry = new CollectorRegistry();
        this.adminClient = createAdminClient(kafkaHostname, kafkaPort);

        if (this.consumer == null) {
            consumer = createNewConsumer(kafkaHostname, kafkaPort);
        }

        registerMetrics();
    }


    public void registerMetrics() {
        gaugeOffsetLag = Gauge.build()
                .name("kafka_broker_consumer_group_offset_lag")
                .help("Offset lag of a topic/partition")
                .labelNames("client_id", "consumer_address", "group_id", "partition", "topic")
                .register();

        gaugeCurrentOffset = Gauge.build()
                .name("kafka_broker_consumer_group_current_offset")
                .help("Current consumed offset of a topic/partition")
                .labelNames("client_id", "consumer_address", "group_id", "partition", "topic")
                .register();

        registry.register(gaugeCurrentOffset);
        registry.register(gaugeOffsetLag);
    }


    private AdminClient createAdminClient(String kafkaHostname, int kafkaPort) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHostname + ":" + kafkaPort);

        return AdminClient.create(props);
    }

    public void updateMetrics() {
        Collection<GroupOverview> groupOverviews = JavaConverters.asJavaCollectionConverter(adminClient.listAllConsumerGroupsFlattened()).asJavaCollection();
        List<String> groups = groupOverviews.stream().map(t -> t.groupId()).collect(Collectors.toList());

        for (String group : groups) {
            AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(group);
            Map<TopicPartition, Object> offsets = JavaConverters.asJavaMapConverter(adminClient.listGroupOffsets(group)).asJava();
            Collection<AdminClient.ConsumerSummary> consumerSummaries = JavaConverters.asJavaCollectionConverter(consumerGroupSummary.consumers().get()).asJavaCollection();

            consumerSummaries.stream().forEach(c -> {
                offsets.forEach((k, v) -> {
                            TopicPartition topicPartition = new TopicPartition(k.topic(), k.partition());
                            Long lag = getLogEndOffset(topicPartition) - new Long(v.toString());

                            gaugeOffsetLag.labels(c.clientId(), c.host(), group, String.valueOf(k.partition()), k.topic()).set(new Double(v.toString()));
                            gaugeCurrentOffset.labels(c.clientId(), c.host(), group, String.valueOf(k.partition()), k.topic()).set(lag);
                        }
                );
            });
        }
    }

    private long getLogEndOffset(TopicPartition topicPartition) {
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        return consumer.position(topicPartition);
    }

    private KafkaConsumer<String, String> createNewConsumer(String kafkaHost, int kafkaPort) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }

}
