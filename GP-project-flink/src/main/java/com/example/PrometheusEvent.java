package com.example;

public class PrometheusEvent extends BaseEvent {
    private static final long serialVersionUID = 1L;

    public String metricName;  // e.g. agent_cpu_avg
    public double value;       // Metric value

    public PrometheusEvent() {
        this.eventType = "PROMETHEUS";
    }
}
