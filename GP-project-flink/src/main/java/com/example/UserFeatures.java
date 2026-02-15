package com.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UserFeatures implements Serializable {
    private static final long serialVersionUID = 1L;
    public String userId;
    public long windowEnd;
    public String windowEndStr;

    // ML Features (individual columns — consumed by PostgreSQL sink)
    public double login_fail_rate;
    public int distinct_ip_count;
    public long api_request_count;
    public double http_4xx_rate;
    public long data_out_bytes;
    public double api_path_entropy;
    public long high_sev_alert_count;
    public double avg_wazuh_level;
    public double avg_cpu_usage;

    // Dual-Payload fields (consumed by Kafka sink for Multi-Modal DL model)
    public List<Double> aggregateVector = new ArrayList<>();
    public List<String> rawEventSequence = new ArrayList<>();

    // DDoS / Log-Flood detection fields
    public long totalEventCount;
    public boolean limitExceeded;

    public UserFeatures() {}

    // Getter for Kafka key-based partitioning
    public String getUserId() {
        return userId;
    }
}
