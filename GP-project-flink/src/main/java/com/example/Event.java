package com.example;

import java.io.Serializable;

/**
 * Unified Event POJO.
 * Note: Logic Assumption - We treat all incoming JSON logs as this single flat class.
 * Fields will be null if they don't apply to the specific source.
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public String userId;       // The JOIN key
    public long timestamp;      // Event time in epoch millis
    public String eventType;    // Discriminator (KEYCLOAK, HAPROXY, WAZUH, PROM)

    // Keycloak specific
    public String keycloak_type; 
    public String keycloak_ip;
    public String keycloak_clientId;

    // HAProxy specific
    public int haproxy_status;
    public String haproxy_method;
    public String haproxy_path;
    public long haproxy_bytes;
    public String haproxy_ua;

    // Wazuh specific
    public int wazuh_level;
    public String wazuh_ruleId;
    
    // Prometheus specific
    public String prom_metricName;
    public double prom_value;

    public Event() {} 
}