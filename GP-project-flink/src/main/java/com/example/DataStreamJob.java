package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

// Using Flink's internal shaded Jackson (available in flink-streaming-java)
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataStreamJob {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Define Sources
        KafkaSource<String> keycloakSource = createKafkaSource("keycloak_events");
        KafkaSource<String> haproxySource = createKafkaSource("haproxy_logs");
        KafkaSource<String> wazuhSource = createKafkaSource("wazuh_alerts");
        KafkaSource<String> promSource = createKafkaSource("prometheus_metrics");

        // 2. Parse and Map
        DataStream<Event> s1 = env.fromSource(keycloakSource, WatermarkStrategy.noWatermarks(), "Keycloak").map(new EventParser("KEYCLOAK"));
        DataStream<Event> s2 = env.fromSource(haproxySource, WatermarkStrategy.noWatermarks(), "HAProxy").map(new EventParser("HAPROXY"));
        DataStream<Event> s3 = env.fromSource(wazuhSource, WatermarkStrategy.noWatermarks(), "Wazuh").map(new EventParser("WAZUH"));
        DataStream<Event> s4 = env.fromSource(promSource, WatermarkStrategy.noWatermarks(), "Prometheus").map(new EventParser("PROMETHEUS"));

        // 3. Union -> Watermark -> Window -> Aggregate
        DataStream<UserFeatures> features = s1.union(s2, s3, s4)
                .filter(e -> e.userId != null)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
                )
                .keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new FeatureAggregator(), new FeatureWindowProcessor());

        // 4. Sink to Postgres
        features.addSink(new CustomJdbcSink());

        env.execute("UEBA Realtime Features");
    }

    private static KafkaSource<String> createKafkaSource(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics(topic)
                .setGroupId("flink-ueba-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    // --- POJOs ---
    public static class Event {
        public String userId;
        public long timestamp;
        public String eventType;
        // Fields for aggregation
        public boolean isLoginError = false;
        public String ipAddress;
        public int httpStatus;
        public String httpPath;
        public long bytesRead;
        public int alertLevel;
        public double cpuValue;
    }

    public static class AggregatorAccumulator {
        public String userId;
        public long loginSuccess = 0;
        public long loginFail = 0;
        public Set<String> ips = new HashSet<>();
        public long apiRequests = 0;
        public long http4xx = 0;
        public long dataBytes = 0;
        public Map<String, Long> pathCounts = new HashMap<>();
        public long highSevAlerts = 0;
        public double alertLevelSum = 0;
        public long alertCount = 0;
        public double cpuSum = 0;
        public long cpuCount = 0;
    }

    public static class UserFeatures {
        public String userId;
        public long windowEnd;
        public double loginFailRate;
        public int distinctIpCount;
        public long apiReqCount;
        public double http4xxRate;
        public long dataOutBytes;
        public double entropy;
        public long highSevAlerts;
        public double avgWazuhLevel;
        public double avgCpu;
    }

    // --- Logic Classes ---

    public static class EventParser implements MapFunction<String, Event> {
        private final String type;
        public EventParser(String type) { this.type = type; }
        
        @Override
        public Event map(String value) {
            Event e = new Event();
            e.eventType = type;
            e.timestamp = System.currentTimeMillis();
            try {
                JsonNode root = mapper.readTree(value);
                
                // Extract User ID
                if (root.has("userId")) e.userId = root.get("userId").asText();
                else if (root.has("jwt_user_id")) e.userId = root.get("jwt_user_id").asText();
                else if (root.has("labels") && root.get("labels").has("userId")) e.userId = root.get("labels").get("userId").asText();

                // Extract Timestamp
                if (root.has("time")) e.timestamp = root.get("time").asLong();
                else if (root.has("timestamp")) {
                    String ts = root.get("timestamp").asText();
                    try {
                        if(ts.contains("T")) e.timestamp = Instant.parse(ts).toEpochMilli();
                        else e.timestamp = Long.parseLong(ts);
                    } catch(Exception ex){}
                }

                // Parse Specifics
                if (type.equals("KEYCLOAK")) {
                    if (root.has("type") && root.get("type").asText().equals("LOGIN_ERROR")) e.isLoginError = true;
                    if (root.has("ipAddress")) e.ipAddress = root.get("ipAddress").asText();
                } else if (type.equals("HAPROXY")) {
                    e.httpStatus = root.path("http_status").asInt(200);
                    e.httpPath = root.path("http_path").asText("");
                    e.bytesRead = root.path("bytes_read").asLong(0);
                } else if (type.equals("WAZUH")) {
                    e.alertLevel = root.path("rule").path("level").asInt(0);
                } else if (type.equals("PROMETHEUS")) {
                    if ("agent_cpu_avg".equals(root.path("metric_name").asText())) {
                        e.cpuValue = root.path("value").asDouble(0.0);
                    }
                }
            } catch (Exception ex) { } // Skip bad JSON
            return e;
        }
    }

    public static class FeatureAggregator implements AggregateFunction<Event, AggregatorAccumulator, AggregatorAccumulator> {
        @Override
        public AggregatorAccumulator createAccumulator() { return new AggregatorAccumulator(); }
        
        @Override
        public AggregatorAccumulator add(Event e, AggregatorAccumulator acc) {
            acc.userId = e.userId;
            if ("KEYCLOAK".equals(e.eventType)) {
                if (e.isLoginError) acc.loginFail++; else acc.loginSuccess++;
                if (e.ipAddress != null) acc.ips.add(e.ipAddress);
            } else if ("HAPROXY".equals(e.eventType)) {
                acc.apiRequests++;
                acc.dataBytes += e.bytesRead;
                if (e.httpStatus >= 400 && e.httpStatus < 500) acc.http4xx++;
                if (e.httpPath != null) acc.pathCounts.merge(e.httpPath, 1L, Long::sum);
            } else if ("WAZUH".equals(e.eventType)) {
                acc.alertCount++;
                acc.alertLevelSum += e.alertLevel;
                if (e.alertLevel >= 10) acc.highSevAlerts++;
            } else if ("PROMETHEUS".equals(e.eventType)) {
                if (e.cpuValue > 0) { acc.cpuSum += e.cpuValue; acc.cpuCount++; }
            }
            return acc;
        }
        @Override
        public AggregatorAccumulator getResult(AggregatorAccumulator acc) { return acc; }
        @Override
        public AggregatorAccumulator merge(AggregatorAccumulator a, AggregatorAccumulator b) { return a; } // Simplified
    }

    public static class FeatureWindowProcessor extends ProcessWindowFunction<AggregatorAccumulator, UserFeatures, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<AggregatorAccumulator> elements, Collector<UserFeatures> out) {
            AggregatorAccumulator acc = elements.iterator().next();
            UserFeatures f = new UserFeatures();
            f.userId = key;
            f.windowEnd = ctx.window().getEnd();
            
            long totalLogins = acc.loginSuccess + acc.loginFail;
            f.loginFailRate = totalLogins == 0 ? 0 : (double)acc.loginFail / totalLogins;
            f.distinctIpCount = acc.ips.size();
            f.apiReqCount = acc.apiRequests;
            f.http4xxRate = acc.apiRequests == 0 ? 0 : (double)acc.http4xx / acc.apiRequests;
            f.dataOutBytes = acc.dataBytes;
            f.highSevAlerts = acc.highSevAlerts;
            f.avgWazuhLevel = acc.alertCount == 0 ? 0 : acc.alertLevelSum / acc.alertCount;
            f.avgCpu = acc.cpuCount == 0 ? 0 : acc.cpuSum / acc.cpuCount;
            
            // Entropy
            double entropy = 0.0;
            for (long count : acc.pathCounts.values()) {
                double p = (double) count / acc.apiRequests;
                entropy -= p * Math.log(p) / Math.log(2);
            }
            f.entropy = entropy;
            
            out.collect(f);
        }
    }

    // Custom JDBC Sink compatible with your limited dependencies
    public static class CustomJdbcSink extends RichSinkFunction<UserFeatures> {
        private Connection conn;
        private PreparedStatement ps;
        private Statement notifyStmt;

        @Override
        public void open(Configuration params) throws Exception {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://postgres:5432/ueba", "flink", "flink");
            
            String sql = "INSERT INTO user_features (userId, windowEnd, login_fail_rate, distinct_ip_count, " +
                         "api_request_count, http_4xx_rate, data_out_bytes, api_path_entropy, " +
                         "high_sev_alert_count, avg_wazuh_level, avg_cpu_usage) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                         "ON CONFLICT (userId, windowEnd) DO UPDATE SET " +
                         "login_fail_rate = EXCLUDED.login_fail_rate, " +
                         "api_request_count = EXCLUDED.api_request_count, " +
                         "high_sev_alert_count = EXCLUDED.high_sev_alert_count";
            ps = conn.prepareStatement(sql);
            notifyStmt = conn.createStatement();
        }

        @Override
        public void invoke(UserFeatures f, Context context) throws Exception {
            ps.setString(1, f.userId);
            ps.setTimestamp(2, new Timestamp(f.windowEnd));
            ps.setDouble(3, f.loginFailRate);
            ps.setInt(4, f.distinctIpCount);
            ps.setLong(5, f.apiReqCount);
            ps.setDouble(6, f.http4xxRate);
            ps.setLong(7, f.dataOutBytes);
            ps.setDouble(8, f.entropy);
            ps.setLong(9, f.highSevAlerts);
            ps.setDouble(10, f.avgWazuhLevel);
            ps.setDouble(11, f.avgCpu);
            
            ps.executeUpdate();

            // Trigger Model
            String payload = "{\"userId\": \"" + f.userId + "\", \"windowEnd\": " + f.windowEnd + "}";
            notifyStmt.execute("NOTIFY model_trigger, '" + payload + "'");
        }

        @Override
        public void close() throws Exception {
            if(ps != null) ps.close();
            if(conn != null) conn.close();
        }
    }
}