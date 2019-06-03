package io.micrometer.core.instrument.binder.kafka;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

public class MockConsumerWithMetrics extends MockConsumer<String, Object> {

    public MockConsumerWithMetrics(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }



    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return null;
    }

    private void ensureNotClosed() {
        if (this.closed())
            throw new IllegalStateException("This consumer has already been closed.");
    }
}
