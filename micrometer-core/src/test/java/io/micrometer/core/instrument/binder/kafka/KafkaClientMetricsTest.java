package io.micrometer.core.instrument.binder.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

class KafkaClientMetricsTest {

    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private Tags tags = Tags.of("app", "myapp", "version", "1");
    private KafkaClientMetrics kafkaClientMetrics = new KafkaClientMetrics();

    @Test
    void verifyConsumerMetricsWithExpectedTags() {
        try (Consumer<String, Object> consumer = createConsumer()) {
            MeterRegistry registry = new SimpleMeterRegistry();
            kafkaClientMetrics.bindTo(registry);
            kafkaClientMetrics.init(new ArrayList<>());
            Map<MetricName, KafkaMetric> metrics = (Map<MetricName, KafkaMetric>) consumer.metrics();
            for(MetricName metricName : metrics.keySet()) {
                kafkaClientMetrics.metricChange(metrics.get(metricName));

                // now the registry should have the meter
                registry.get(kafkaClientMetrics.createId(metrics.get(metricName))).tags(tags).functionCounter();
            }
        }
    }

    private Consumer<String, Object> createConsumer() {
        // props are not used in this case but here to show how to configure the MetricReporter
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MicrometerTestConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // this is the configuration needed to collect metrics from a kafka consumer.
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics");

        // we mock the consumer with some pre defined KafkaMetrics
        Consumer<String, Object> consumer = new MockConsumerWithMetrics(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

}
