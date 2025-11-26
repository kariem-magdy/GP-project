package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class UserFeaturesJob {
    
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // --- 1. Sources ---
        
        // We use 'valueOnly' because our JSONDeserializer implements the standard DeserializationSchema
        // Use kafka:9092 for inter-container communication
        KafkaSource<JsonNode> keycloakSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("keycloak_events")
                .setGroupId("ueba-group-keycloak")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> haproxySource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("haproxy_logs")
                .setGroupId("ueba-group-haproxy")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> wazuhSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("wazuh_alerts")
                .setGroupId("ueba-group-wazuh")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        KafkaSource<JsonNode> prometheusSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("prometheus_metrics")
                .setGroupId("ueba-group-prom")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializer()))
                .build();

        // --- 2. Map Sources to Unified Event POJO with Watermarks ---
        
        // Define watermark strategy once with idleness timeout
        // This allows windows to trigger even if some sources haven't sent data
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.timestamp)
            .withIdleness(Duration.ofSeconds(30));

        DataStream<Event> keycloakStream = env.fromSource(
                keycloakSource, 
                WatermarkStrategy.noWatermarks(), 
                "Keycloak"
            )
            .map(new KeycloakEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Event> haproxyStream = env.fromSource(
                haproxySource, 
                WatermarkStrategy.noWatermarks(), 
                "HAProxy"
            )
            .map(new HaproxyEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);
        
        DataStream<Event> wazuhStream = env.fromSource(
                wazuhSource, 
                WatermarkStrategy.noWatermarks(), 
                "Wazuh"
            )
            .map(new WazuhEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Event> prometheusStream = env.fromSource(
                prometheusSource, 
                WatermarkStrategy.noWatermarks(), 
                "Prometheus"
            )
            .map(new PrometheusEventMapper())
            .filter(e -> e.userId != null && e.timestamp > 0)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        // --- 3. Union streams (watermarks already assigned) ---
        
        DataStream<Event> unifiedStream = keycloakStream
            .union(haproxyStream, wazuhStream, prometheusStream);

        // --- 4. Windowing & Aggregation ---
        // Add logging to see if events are flowing through
        DataStream<Event> loggedStream = unifiedStream.map(e -> {
            System.out.println("Processing event: " + e.eventType + " for user " + e.userId + " at " + e.timestamp);
            return e;
        });
        
        DataStream<UserFeatures> features = loggedStream
            .keyBy(e -> e.userId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new FeatureAggregator(), new FeatureWindowProcessor());

        // --- 5. Sink ---
        
        // Ensure you have updated the JdbcUserFeaturesSink class to match the constructor
        features.addSink(new JdbcUserFeaturesSink());

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

    // --- Mappers ---

    public static class KeycloakEventMapper implements MapFunction<JsonNode, Event> {
        @Override
        public Event map(JsonNode node) {
            Event e = new Event();
            if (node == null) return e;
            e.eventType = "KEYCLOAK";
            e.userId = node.path("userId").asText(null);
            e.timestamp = node.path("time").asLong(System.currentTimeMillis());
            e.keycloak_type = node.path("type").asText();
            e.keycloak_ip = node.path("ipAddress").asText();
            e.keycloak_clientId = node.path("clientId").asText();
            return e;
        }
    }

    public static class HaproxyEventMapper implements MapFunction<JsonNode, Event> {
        @Override
        public Event map(JsonNode node) {
            Event e = new Event();
            if (node == null) return e;
            e.eventType = "HAPROXY";
            e.userId = node.path("jwt_user_id").asText(null);
            try {
                String ts = node.path("timestamp").asText();
                e.timestamp = Instant.parse(ts).toEpochMilli();
            } catch (Exception ex) { e.timestamp = System.currentTimeMillis(); }
            
            e.haproxy_method = node.path("http_method").asText();
            e.haproxy_path = node.path("http_path").asText();
            e.haproxy_status = node.path("http_status").asInt();
            e.haproxy_bytes = node.path("bytes_read").asLong();
            e.haproxy_ua = node.path("user_agent").asText();
            return e;
        }
    }

    public static class WazuhEventMapper implements MapFunction<JsonNode, Event> {
        @Override
        public Event map(JsonNode node) {
            Event e = new Event();
            if (node == null) return e;
            e.eventType = "WAZUH";
            e.userId = node.path("userId").asText(null);
            try {
                String ts = node.path("timestamp").asText();
                e.timestamp = Instant.parse(ts).toEpochMilli();
            } catch (Exception ex) { e.timestamp = System.currentTimeMillis(); }
            
            e.wazuh_level = node.path("rule").path("level").asInt();
            e.wazuh_ruleId = node.path("rule").path("id").asText();
            return e;
        }
    }

    public static class PrometheusEventMapper implements MapFunction<JsonNode, Event> {
        @Override
        public Event map(JsonNode node) {
            Event e = new Event();
            if (node == null) return e;
            e.eventType = "PROMETHEUS";
            // Assume userId is in labels based on producer logic
            e.userId = node.path("labels").path("userId").asText(null);
            e.timestamp = node.path("timestamp").asLong(System.currentTimeMillis());
            e.prom_metricName = node.path("metric_name").asText();
            e.prom_value = node.path("value").asDouble();
            return e;
        }
    }

    // --- Aggregators ---

    public static class FeatureAggregator implements AggregateFunction<Event, PerFiveMinAgg, PerFiveMinAgg> {
        @Override
        public PerFiveMinAgg createAccumulator() { return new PerFiveMinAgg(); }

        @Override
        public PerFiveMinAgg add(Event e, PerFiveMinAgg acc) {
            if (acc.userId == null && e.userId != null) acc.userId = e.userId;

            if ("KEYCLOAK".equals(e.eventType)) {
                if ("LOGIN_ERROR".equals(e.keycloak_type)) acc.loginFailCount++;
                else if ("LOGIN".equals(e.keycloak_type)) acc.loginSuccessCount++;
                if (e.keycloak_ip != null) acc.distinctIps.add(e.keycloak_ip);
            }
            else if ("HAPROXY".equals(e.eventType)) {
                acc.apiRequestCount++;
                acc.dataOutBytes += e.haproxy_bytes;
                if (e.haproxy_status >= 400 && e.haproxy_status < 500) acc.http4xxCount++;
                if (e.haproxy_path != null) acc.apiPathCounts.merge(e.haproxy_path, 1L, Long::sum);
            }
            else if ("WAZUH".equals(e.eventType)) {
                acc.totalAlerts++;
                acc.alertLevelSum += e.wazuh_level;
                if (e.wazuh_level >= 10) acc.highSevAlertCount++;
            }
            else if ("PROMETHEUS".equals(e.eventType)) {
                if ("agent_cpu_avg".equals(e.prom_metricName)) {
                    acc.cpuSum += e.prom_value;
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

    public static class FeatureWindowProcessor extends ProcessWindowFunction<PerFiveMinAgg, UserFeatures, String, TimeWindow> {
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

            System.out.println("Window triggered for user: " + key + ", windowEnd: " + feat.windowEnd);
            out.collect(feat);
        }
    }
}
