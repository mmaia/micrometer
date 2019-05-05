package io.micrometer.core.instrument.binder.kafka;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.lang.NonNullApi;
import io.micrometer.core.lang.NonNullFields;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Kafka client metrics collected directly from the kafka MetricsReporter
 * {@link org.apache.kafka.common.metrics.MetricsReporter}
 *
 * @see <a href="https://kafka.apache.org/documentation/#monitoring">Kafka Metrics Reporter</a>
 * @author Marcos Maia
 */
@NonNullApi
@NonNullFields
public class KafkaClientMetrics implements MetricsReporter {

    private MeterRegistry registry;

    @Override
    public void init(List<KafkaMetric> metrics) {}

    /**
     * Emitted by MetricsReporter {@link org.apache.kafka.common.metrics.MetricsReporter#metricChange(KafkaMetric)}
     * @param metric that has change with new value.
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        String metricName = metric.metricName().name();
        if(metricName.contains("total")) {
            registerCounter(metric);
        } else if(metricName.contains("time") ||
                metricName.contains("latency")) {
            registerTimeGauge(metric, TimeUnit.MILLISECONDS);
        }
        else { // fall back is a gauge which is more generic.
            registerGauge(metric);
        }
    }

    /**
     * TODO - implement this
     * @param metric
     */
    @Override
    public void metricRemoval(KafkaMetric metric) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    public void registerGauge(KafkaMetric kafkaMetric) {
        if(registry == null) {
            registry = Metrics.globalRegistry;
        }
        final AtomicReference<Gauge> gauge = new AtomicReference<>();
        gauge.set(Gauge.builder( createId(kafkaMetric), kafkaMetric, m -> safeDouble(m.metricValue()) )
                .description(kafkaMetric.metricName().description())
                . tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    public void registerCounter(KafkaMetric kafkaMetric) {
        if(registry == null) {
            registry = Metrics.globalRegistry;
        }
        final AtomicReference<FunctionCounter> counter = new AtomicReference<>();
        counter.set(FunctionCounter
                .builder(createId(kafkaMetric), kafkaMetric, m -> safeDouble(m.metricValue()))
                .description(kafkaMetric.metricName().description())
                .tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    private void registerTimeGauge(KafkaMetric kafkaMetric, TimeUnit timeUnit) {
        if(registry == null) {
            registry = Metrics.globalRegistry;
        }
        final AtomicReference<TimeGauge> timeGauge = new AtomicReference<>();
        timeGauge.set(TimeGauge.builder(createId(kafkaMetric), kafkaMetric, timeUnit,
                m -> safeDouble(m.metricValue()))
                .description(kafkaMetric.metricName().description())
                .tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    /**
     * Creates a unique identifier to the metric giving it a prefix + name + tags flattened separated by "." + group
     * so they're guaranteed to be unique.
     * @param metric the kafka metric to create an ID for.
     * @return a unique identifier for that metric.
     */
    String createId(KafkaMetric metric) {
        Collection<String> tags = metric.metricName().tags().values();
        String joinedTags = String.join(".", tags.<String>toArray(new String[tags.size()]));
        String metricName = metric.metricName().name();
        String group = metric.metricName().group();
        return prefix(group) + "." + metricName + "."+ group + "." + joinedTags;
    }

    /**
     * Prefixes the metric based on it's group as defined by kafka documentation, check link below for more details.
     * @see <a href="https://kafka.apache.org/documentation/#selector_monitoring">Kafka common monitoring metrics</a>
     * @param group the group of the received metric
     * @return a prefix to be used by the metric name when registering to micrometer.
     */
    String prefix(String group) {
        if(group.contains("producer")) {
            return "kafka.producer";
        } else if(group.contains("consumer")) {
            return "kafka.consumer";
        } else if(group.contains("stream")) {
            return "kafka.streams";
        } else if(group.contains("connect")) {
            return "kafka.connect";
        } else {
            return "kafka";
        }
    }

    private Iterable<Tag> parseTags(Map<String, String> tags, String group) {
        List<Tag> tagList = new ArrayList<>(tags.size());
        for (String key : tags.keySet()) {
            tagList.add(Tag.of(key, tags.get(key)));
        }
        tagList.add(Tag.of("group",group));
        return tagList;
    }

    /**
     * Returns casted double or 0.0 in case the value can't be converted. Avoids runtime exceptions.
     *
     * @param doubleValue the value to be casted to Double
     * @return the value as double or if any exceptions the value 0.0 instead.
     */
    double safeDouble(Object doubleValue) {
        double result = Double.NaN;
        try {
            result = (Double) doubleValue;
            return result;
        } catch (Exception e) {
            return result;
        }
    }
}
