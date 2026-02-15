package com.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PerFiveMinAgg implements Serializable {
    private static final long serialVersionUID = 1L;
    public String userId;
    
    // Aggregations
    public long loginFailCount = 0;
    public long loginSuccessCount = 0;
    public Set<String> distinctIps = new HashSet<>();
    
    public long apiRequestCount = 0;
    public long http4xxCount = 0;
    public long dataOutBytes = 0;
    public Map<String, Long> apiPathCounts = new HashMap<>(); // For entropy
    
    public long highSevAlertCount = 0; // Wazuh level >= 10
    public double alertLevelSum = 0;
    public long totalAlerts = 0;
    
    public double cpuSum = 0;
    public long cpuCount = 0;

    // Chronological sequence of lightweight event descriptions for the ML sequence model
    public List<String> rawEventSequence = new ArrayList<>();

    // Total events seen in this window (never capped — used for DDoS/flood detection)
    public long totalEventCount = 0;

    public PerFiveMinAgg() {}
}