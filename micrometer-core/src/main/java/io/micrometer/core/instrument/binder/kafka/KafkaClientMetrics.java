package io.micrometer.core.instrument.binder.kafka;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNullApi;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Kafka client metrics collected directly from the kafka MetricsReporter
 * {@link org.apache.kafka.common.metrics.MetricsReporter}
 *
 * @author Marcos Maia
 * @see <a href="https://kafka.apache.org/documentation/#monitoring">Kafka Metrics Reporter</a>
 */
@NonNullApi
public class KafkaClientMetrics implements MetricsReporter, MeterBinder {

    private MeterRegistry registry;

    private List<KafkaMetric> metrics;

    /**
     * We get a reference to global registry from micrometer as this object instantiation is controlled by kafka clients
     * that have this reporter registered.
     * @param metrics list of metrics, although it's commonly an empty list.
     */
    @Override
    public void init(List<KafkaMetric> metrics) {
        // TODO - there's possibly a better way.
        if (registry == null) {
            registry = Metrics.globalRegistry;
        }
        this.metrics = metrics;
    }

    /**
     * This should be ok but when this class is instantiated by the kafka client and initial metrics are passed in to
     * {@link KafkaClientMetrics#metricChange} the binder is not yet executed (at least in spring boot applications) 
     * and causes the registration to throw NullPointerExceptions
     * @param registry to bind
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Emitted by MetricsReporter {@link org.apache.kafka.common.metrics.MetricsReporter#metricChange(KafkaMetric)}
     * @param metric {@link KafkaMetric} reference that has changed with it's details.
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        String metricName = metric.metricName().name();

        // keep the reference to be reused by micrometer meters to collect the metrics.
        this.metrics.add(metric);

        if (metricName.contains("total")) {
            registerCounter(metric);
        } else if (metricName.contains("time") ||
                metricName.contains("latency")) {
            registerTimeGauge(metric, TimeUnit.MILLISECONDS);
        } else { // fall back is a gauge which is more generic and also used for rate, ratio, this can possibly by improved.
            registerGauge(metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        metrics = metrics.stream()
                .filter( metricRef -> ! metricRef.metricName().name().equalsIgnoreCase(metric.metricName().name()) )
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        // we dont' need it for now.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // we don't need it for now.
    }

    public void registerGauge(KafkaMetric kafkaMetric) {
        final AtomicReference<Gauge> gauge = new AtomicReference<>();
        gauge.set(Gauge.builder(createId(kafkaMetric), kafkaMetric, this::safeDouble)
                .description(kafkaMetric.metricName().description())
                .tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    public void registerCounter(KafkaMetric kafkaMetric) {
        final AtomicReference<FunctionCounter> counter = new AtomicReference<>();
        counter.set(FunctionCounter
                .builder(createId(kafkaMetric), kafkaMetric, this::safeDouble)
                .description(kafkaMetric.metricName().description())
                .tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    private void registerTimeGauge(KafkaMetric kafkaMetric, TimeUnit timeUnit) {
        final AtomicReference<TimeGauge> timeGauge = new AtomicReference<>();
        timeGauge.set(TimeGauge.builder(createId(kafkaMetric), kafkaMetric, timeUnit, this::safeDouble)
                .description(kafkaMetric.metricName().description())
                .tags(parseTags(kafkaMetric.metricName().tags(), kafkaMetric.metricName().group()))
                .register(registry));
    }

    /**
     * Creates a unique identifier to the metric giving it a prefix + name flattened separated by "."
     * @param metric the kafka metric to create an ID for.
     * @return an identifier for the metric based on it's type and metric name.
     * 
     * @see <a href="https://kafka.apache.org/documentation/#selector_monitoring">Kafka common monitoring metrics</a>
     */
    String createId(KafkaMetric metric) {
        String metricName = metric.metricName().name();
        String group = metric.metricName().group();
        return prefix(group) + "." + sanitize(metricName);
    }

    private String sanitize(String metricName) {
        return metricName.replaceAll("-", ".");
    }

    /**
     * Prefixes the metric based on it's group as defined by kafka documentation, check link below for more details.
     * @param group the group of the received metric
     * @return a prefix to be used by the metric name when registering to micrometer.
     * 
     * @see <a href="https://kafka.apache.org/documentation/#selector_monitoring">Kafka common monitoring metrics</a>
     */
    String prefix(String group) {
        if (group.contains("producer-metrics")) {
            return "kafka.producer";
        } else if (group.contains("consumer-metrics")) {
            return "kafka.consumer";
        } else if (group.contains("stream-metrics")) {
            return "kafka.streams";
        } else if (group.contains("connect-metrics")) {
            return "kafka.connect";
        } else {
            return "kafka";
        }
    }

    /**
     * Returs Iterable list of {@link io.micrometer.core.instrument.Tag} extracted from the {@link KafkaMetric} reference.
     * @param tags  map with tags from the kafka metric
     * @param group we add group to tags so we can easily select / filter by type of kafka client.
     * @return list of micrometer Tag that can be used to register the metric.
     */
    private Iterable<Tag> parseTags(Map<String, String> tags, String group) {
        List<Tag> tagList = new ArrayList<>(tags.size());
        for (String key : tags.keySet()) {
            tagList.add(Tag.of(key, tags.get(key)));
        }
        tagList.add(Tag.of("group", group));
        return tagList;
    }

    /**
     * Returns Double value representing current most recently collected value from metric
     * or 0.0 in case the value can't be converted. Avoids runtime exceptions.
     * @param kafkaMetric the metric reference to read the current value from
     * @return the value as Double or if any exceptions the value {@link Double#NaN} instead.
     */
    double safeDouble(KafkaMetric kafkaMetric) {
        double result = Double.NaN;
        try {
            result = (Double) kafkaMetric.metricValue();
            return result;
        } catch (Exception e) {
            return result;
        }
    }
}
