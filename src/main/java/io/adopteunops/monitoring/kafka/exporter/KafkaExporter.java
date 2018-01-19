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

import io.prometheus.client.Gauge;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static scala.collection.JavaConverters.asJavaCollectionConverter;
import static scala.collection.JavaConverters.mapAsJavaMapConverter;

class KafkaExporter {
    private final Gauge gaugeOffsetLag;
    private final Gauge gaugeCurrentOffset;

    private AdminClient adminClient;
    private final KafkaConsumer<String, String> consumer;
    private final Pattern groupBlacklistPattern;

    private final String kafkaHostname;
    private final int kafkaPort;
    
    public KafkaExporter(String kafkaHostname, int kafkaPort, String groupBlacklistRegexp) {
        this.kafkaHostname = kafkaHostname;
        this.kafkaPort = kafkaPort;
        this.adminClient = createAdminClient(kafkaHostname, kafkaPort);
        this.consumer = createNewConsumer(kafkaHostname, kafkaPort);
        this.groupBlacklistPattern = Pattern.compile(groupBlacklistRegexp);
        this.gaugeOffsetLag = Gauge.build()
                .name("kafka_broker_consumer_group_offset_lag")
                .help("Offset lag of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();

        this.gaugeCurrentOffset = Gauge.build()
                .name("kafka_broker_consumer_group_current_offset")
                .help("Current consumed offset of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();
    }

    private AdminClient createAdminClient(String kafkaHostname, int kafkaPort) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostname + ":" + kafkaPort);

        return AdminClient.create(props);
    }

   synchronized void updateMetrics() {

        try {

            Collection<GroupOverview> groupOverviews = asJavaCollectionConverter(adminClient.listAllConsumerGroupsFlattened()).asJavaCollection();

            List<String> groups = groupOverviews.stream()
                    .map(GroupOverview::groupId)
                    .filter(g -> !groupBlacklistPattern.matcher(g).matches())
                    .collect(toList());

            groups.forEach(group -> {
                Map<TopicPartition, Object> offsets = mapAsJavaMapConverter(adminClient.listGroupOffsets(group)).asJava();
                offsets.forEach((k, v) -> {
                    TopicPartition topicPartition = new TopicPartition(k.topic(), k.partition());
                    Long currentOffset = new Long(v.toString());
                    Long lag = getLogEndOffset(topicPartition) - currentOffset;
                    String partition = String.valueOf(k.partition());

                    gaugeOffsetLag.labels(group, partition, k.topic()).set(lag);
                    gaugeCurrentOffset.labels(group, partition, k.topic()).set(currentOffset);
                });
            });

        } catch (java.lang.RuntimeException ex) {
            ex.printStackTrace();
            this.adminClient = createAdminClient(this.kafkaHostname, this.kafkaPort);
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
