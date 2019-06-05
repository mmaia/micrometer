package io.micrometer.core.instrument.binder.kafka;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;

public class MockConsumerWithMetrics extends MockConsumer<String, Object> {

    public MockConsumerWithMetrics(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }

    /**
     * Mocked metrics, we want to test metrics integration so this is where we can mock some metrics from
     * kafka clients to test our micrometer implementation.
     * @return {@link java.util.HashMap} with keys of type: {@link org.apache.kafka.common.MetricName} and values of type: {@link org.apache.kafka.common.metrics.KafkaMetric}
     */
    @Override
    public Map<MetricName, KafkaMetric> metrics() {
        Map<MetricName, KafkaMetric> metrics = new HashMap<>();
        ensureNotClosed();
        HashMap<String, String> tags = new HashMap<>();
        tags.put("app", "myapp");
        tags.put("version", "1");
        MetricName metricName = new MetricName("total-ticks", "group", "description", tags);
        MetricConfig config = new MetricConfig();
        // metric value provider.
        KafkaMetric kafkametric = new KafkaMetric(new Object(), metricName, new MockValueProvider() , config, Time.SYSTEM);
        metrics.put(metricName, kafkametric);
        return metrics;
    }

    private void ensureNotClosed() {
        if (this.closed())
            throw new IllegalStateException("This consumer has already been closed.");
    }
}

class MockValueProvider implements Measurable {
    @Override
    public double measure(MetricConfig config, long now) {
        return 0;
    }
}
