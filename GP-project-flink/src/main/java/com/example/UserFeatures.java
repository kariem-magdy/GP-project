package com.example;

import java.io.Serializable;

public class UserFeatures implements Serializable {
    private static final long serialVersionUID = 1L;
    public String userId;
    public long windowEnd;
    public String windowEndStr;

    // ML Features
    public double login_fail_rate;
    public int distinct_ip_count;
    public long api_request_count;
    public double http_4xx_rate;
    public long data_out_bytes;
    public double api_path_entropy;
    public long high_sev_alert_count;
    public double avg_wazuh_level;
    public double avg_cpu_usage;

    public UserFeatures() {}

    // Getter for Kafka key-based partitioning
    public String getUserId() {
        return userId;
    }
}