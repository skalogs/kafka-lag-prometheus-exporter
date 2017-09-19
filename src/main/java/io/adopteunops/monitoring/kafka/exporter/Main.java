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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.prometheus.client.exporter.MetricsServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Main {

    @Parameter(names = "--kafka-host", description = "Kafka hostname", required = true)
    public String kafkaHostname;

    @Parameter(names = "--kafka-port", description = "Kafka port")
    public int kafkaPort = 9092;

    @Parameter(names = "--port", description = "Exporter port")
    public int port = 7979;

    @Parameter(names = "--group-blacklist-regexp", description = "Consumer group blacklist regexp")
    public String groupBlacklistRegexp = "console-consumer.*";

    @Parameter(names = "--help", help = true)
    private boolean help = false;

    public static void main(String... args) throws Exception {
        Main main = new Main();
        JCommander jcommander = JCommander.newBuilder()
                .addObject(main)
                .build();

        jcommander.parse(args);

        if (main.help) {
            jcommander.usage();
        } else {
            KafkaExporter kafkaExporter = new KafkaExporter(main.kafkaHostname, main.kafkaPort, main.groupBlacklistRegexp);
            MetricsServlet metricsServlet = new MetricsServlet() {
                @Override
                protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                    kafkaExporter.updateMetrics();
                    super.doGet(req, resp);
                }
            };

            ExposePrometheusMetricsServer prometheusMetricServlet = new ExposePrometheusMetricsServer(main.port, metricsServlet);
            prometheusMetricServlet.start();
        }
    }
}
