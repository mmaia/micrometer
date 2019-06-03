package io.micrometer.core.instrument.binder.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

class KafkaClientMetricsTest {

    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static int consumerCount = 0;

    private Tags tags = Tags.of("app", "myapp", "version", "1");
    private KafkaClientMetrics kafkaClientMetrics = new KafkaClientMetrics();

    @Test
    void verifyConsumerMetricsWithExpectedTags() {
        try (Consumer<Long, String> consumer = createConsumer()) {
            MeterRegistry registry = new SimpleMeterRegistry();
            kafkaClientMetrics.bindTo(registry);

        }
    }


    @Test
    public void testBootstrap() {
        String ipAddress = "140.211.11.105";
        String hostName = "www.example.com";
        Cluster cluster = Cluster.bootstrap(Arrays.asList(
                new InetSocketAddress(ipAddress, 9002),
                new InetSocketAddress(hostName, 9002)
        ));
        Set<String> expectedHosts = Utils.mkSet(ipAddress, hostName);
        Set<String> actualHosts = new HashSet<>();
        for (Node node : cluster.nodes())
            actualHosts.add(node.host());
        assertEquals(expectedHosts, actualHosts);
    }


    private Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MicrometerTestConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics");

        Consumer<Long, String> consumer = new MockConsumerWithMetrics<>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(Collections.singletonList(TOPIC));
        consumerCount++;
        return consumer;
    }

}
