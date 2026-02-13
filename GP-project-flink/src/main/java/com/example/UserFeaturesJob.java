package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class UserFeaturesJob {

    private static final Logger LOG = LoggerFactory.getLogger(UserFeaturesJob.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String kafkaBrokers = params.get("kafka.bootstrap.servers", "kafka:9092");
        final String dbUrl = params.get("db.url", "jdbc:postgresql://postgres:5432/ueba");
        final String dbUser = params.get("db.user", "flink");
        final String dbPassword = params.get("db.password", "flink");

        LOG.info("Starting UEBA Feature Engineering Job");
        LOG.info("Kafka brokers: {}", kafkaBrokers);
        LOG.info("Database URL: {}", dbUrl);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Make parameters available to all operators via global job parameters
        env.getConfig().setGlobalJobParameters(params);

        // Checkpointing (save state every 3 minutes)
        env.enableCheckpointing(180000);
        LOG.info("Checkpointing enabled with 3-minute interval");

        // --- 1. Sources ---

        KafkaSource<JsonNode> keycloakSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("keycloak_events")
                .setGroupId("ueba-group-keycloak")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> haproxySource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("haproxy_logs")
                .setGroupId("ueba-group-haproxy")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> wazuhSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("wazuh_alerts")
                .setGroupId("ueba-group-wazuh")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> prometheusSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("prometheus_metrics")
                .setGroupId("ueba-group-prom")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        // --- 2. Map Sources to Typed Event POJOs with Watermarks ---

        WatermarkStrategy<BaseEvent> watermarkStrategy = WatermarkStrategy
            .<BaseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.timestamp)
            .withIdleness(Duration.ofSeconds(30));

        DataStream<BaseEvent> keycloakStream = env.fromSource(
                keycloakSource,
                WatermarkStrategy.noWatermarks(),
                "Keycloak"
            )
            .map(new KeycloakEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<BaseEvent> haproxyStream = env.fromSource(
                haproxySource,
                WatermarkStrategy.noWatermarks(),
                "HAProxy"
            )
            .map(new HaproxyEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<BaseEvent> wazuhStream = env.fromSource(
                wazuhSource,
                WatermarkStrategy.noWatermarks(),
                "Wazuh"
            )
            .map(new WazuhEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<BaseEvent> prometheusStream = env.fromSource(
                prometheusSource,
                WatermarkStrategy.noWatermarks(),
                "Prometheus"
            )
            .map(new PrometheusEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        // --- 3. Union streams ---

        DataStream<BaseEvent> unifiedStream = keycloakStream
            .union(haproxyStream, wazuhStream, prometheusStream);

        // --- 4. Windowing & Aggregation ---

        DataStream<BaseEvent> loggedStream = unifiedStream.map(e -> {
            LOG.debug("Processing event: {} for user {} at {}", e.eventType, e.userId, e.timestamp);
            return e;
        });

        // Upgrade #2: Sliding Window (5-minute window, slides every 1 minute)
        // Upgrade #9: Using Duration API instead of deprecated Time
        DataStream<UserFeatures> features = loggedStream
            .keyBy(e -> e.userId)
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofMinutes(1)))
            .aggregate(new FeatureAggregator(), new FeatureWindowProcessor());

        // --- 5. Sink ---
        // Upgrade #3: Pass DB connection parameters from ParameterTool
        features.addSink(new JdbcUserFeaturesSink(dbUrl, dbUser, dbPassword));

        env.execute("UEBA Feature Engineering");
    }

    // --- Helpers ---

    public static class JSONDeserializer implements org.apache.flink.api.common.serialization.DeserializationSchema<JsonNode> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override
        public JsonNode deserialize(byte[] message) {
            try { return mapper.readTree(message); } catch (Exception e) { return null; }
        }
        @Override
        public boolean isEndOfStream(JsonNode nextElement) { return false; }
        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<JsonNode> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(JsonNode.class);
        }
    }

    // --- Mappers to convert JsonNode to typed BaseEvent subclasses ---

    public static class KeycloakEventMapper implements MapFunction<JsonNode, BaseEvent> {
        @Override
        public BaseEvent map(JsonNode node) {
            KeycloakEvent e = new KeycloakEvent();
            if (node == null) return e;
            e.userId = node.path("userId").asText(null);
            e.timestamp = node.path("time").asLong(System.currentTimeMillis());
            e.type = node.path("type").asText();
            e.ip = node.path("ipAddress").asText();
            e.clientId = node.path("clientId").asText();
            return e;
        }
    }

    public static class HaproxyEventMapper implements MapFunction<JsonNode, BaseEvent> {
        @Override
        public BaseEvent map(JsonNode node) {
            HaproxyEvent e = new HaproxyEvent();
            if (node == null) return e;
            e.userId = node.path("jwt_user_id").asText(null);
            try {
                String ts = node.path("timestamp").asText();
                e.timestamp = Instant.parse(ts).toEpochMilli();
            } catch (Exception ex) { e.timestamp = System.currentTimeMillis(); }

            e.method = node.path("http_method").asText();
            e.path = node.path("http_path").asText();
            e.status = node.path("http_status").asInt();
            e.bytes = node.path("bytes_read").asLong();
            e.userAgent = node.path("user_agent").asText();
            return e;
        }
    }

    public static class WazuhEventMapper implements MapFunction<JsonNode, BaseEvent> {
        @Override
        public BaseEvent map(JsonNode node) {
            WazuhEvent e = new WazuhEvent();
            if (node == null) return e;
            e.userId = node.path("userId").asText(null);
            try {
                String ts = node.path("timestamp").asText();
                e.timestamp = Instant.parse(ts).toEpochMilli();
            } catch (Exception ex) { e.timestamp = System.currentTimeMillis(); }

            e.level = node.path("rule").path("level").asInt();
            e.ruleId = node.path("rule").path("id").asText();
            return e;
        }
    }

    public static class PrometheusEventMapper implements MapFunction<JsonNode, BaseEvent> {
        @Override
        public BaseEvent map(JsonNode node) {
            PrometheusEvent e = new PrometheusEvent();
            if (node == null) return e;
            e.userId = node.path("labels").path("userId").asText(null);
            e.timestamp = node.path("timestamp").asLong(System.currentTimeMillis());
            e.metricName = node.path("metric_name").asText();
            e.value = node.path("value").asDouble();
            return e;
        }
    }

    // --- Aggregators ---

    public static class FeatureAggregator implements AggregateFunction<BaseEvent, PerFiveMinAgg, PerFiveMinAgg> {
        @Override
        public PerFiveMinAgg createAccumulator() { return new PerFiveMinAgg(); }

        @Override
        public PerFiveMinAgg add(BaseEvent event, PerFiveMinAgg acc) {
            if (acc.userId == null && event.userId != null) acc.userId = event.userId;

            if (event instanceof KeycloakEvent) {
                KeycloakEvent e = (KeycloakEvent) event;
                if ("LOGIN_ERROR".equals(e.type)) acc.loginFailCount++;
                else if ("LOGIN".equals(e.type)) acc.loginSuccessCount++;
                if (e.ip != null) acc.distinctIps.add(e.ip);
            }
            else if (event instanceof HaproxyEvent) {
                HaproxyEvent e = (HaproxyEvent) event;
                acc.apiRequestCount++;
                acc.dataOutBytes += e.bytes;
                if (e.status >= 400 && e.status < 500) acc.http4xxCount++;
                if (e.path != null) acc.apiPathCounts.merge(e.path, 1L, Long::sum);
            }
            else if (event instanceof WazuhEvent) {
                WazuhEvent e = (WazuhEvent) event;
                acc.totalAlerts++;
                acc.alertLevelSum += e.level;
                if (e.level >= 10) acc.highSevAlertCount++;
            }
            else if (event instanceof PrometheusEvent) {
                PrometheusEvent e = (PrometheusEvent) event;
                if ("agent_cpu_avg".equals(e.metricName)) {
                    acc.cpuSum += e.value;
                    acc.cpuCount++;
                }
            }
            return acc;
        }

        @Override
        public PerFiveMinAgg getResult(PerFiveMinAgg acc) { return acc; }

        @Override
        public PerFiveMinAgg merge(PerFiveMinAgg a, PerFiveMinAgg b) {
            a.loginFailCount += b.loginFailCount;
            a.loginSuccessCount += b.loginSuccessCount;
            a.distinctIps.addAll(b.distinctIps);
            a.apiRequestCount += b.apiRequestCount;
            a.dataOutBytes += b.dataOutBytes;
            a.http4xxCount += b.http4xxCount;
            a.highSevAlertCount += b.highSevAlertCount;
            a.alertLevelSum += b.alertLevelSum;
            a.totalAlerts += b.totalAlerts;
            a.cpuSum += b.cpuSum;
            a.cpuCount += b.cpuCount;
            b.apiPathCounts.forEach((k, v) -> a.apiPathCounts.merge(k, v, Long::sum));
            return a;
        }
    }

    // Runs once when the window closes
    public static class FeatureWindowProcessor extends ProcessWindowFunction<PerFiveMinAgg, UserFeatures, String, TimeWindow> {
        private static final Logger LOG = LoggerFactory.getLogger(FeatureWindowProcessor.class);

        @Override
        public void process(String key, Context context, Iterable<PerFiveMinAgg> elements, Collector<UserFeatures> out) {
            PerFiveMinAgg agg = elements.iterator().next();
            UserFeatures feat = new UserFeatures();

            feat.userId = key;
            feat.windowEnd = context.window().getEnd();

            long totalLogins = agg.loginSuccessCount + agg.loginFailCount;
            feat.login_fail_rate = totalLogins == 0 ? 0 : (double) agg.loginFailCount / totalLogins;

            feat.api_request_count = agg.apiRequestCount;
            feat.http_4xx_rate = agg.apiRequestCount == 0 ? 0 : (double) agg.http4xxCount / agg.apiRequestCount;

            // Shannon Entropy for API paths
            double entropy = 0.0;
            if (agg.apiRequestCount > 0 && !agg.apiPathCounts.isEmpty()) {
                for (long count : agg.apiPathCounts.values()) {
                    double p = (double) count / agg.apiRequestCount;
                    if (p > 0) {
                        entropy -= p * (Math.log(p) / Math.log(2));
                    }
                }
            }
            feat.api_path_entropy = entropy;

            feat.high_sev_alert_count = agg.highSevAlertCount;
            feat.avg_wazuh_level = agg.totalAlerts == 0 ? 0 : agg.alertLevelSum / agg.totalAlerts;

            feat.avg_cpu_usage = agg.cpuCount == 0 ? 0 : agg.cpuSum / agg.cpuCount;
            feat.data_out_bytes = agg.dataOutBytes;
            feat.distinct_ip_count = agg.distinctIps.size();

            LOG.info("Window triggered for user: {}, windowEnd: {}", key, feat.windowEnd);
            out.collect(feat);
        }
    }
}
