package com.example;

public class WazuhEvent extends BaseEvent {
    private static final long serialVersionUID = 1L;

    public int level;          // Alert severity level
    public String ruleId;      // Wazuh rule ID

    public WazuhEvent() {
        this.eventType = "WAZUH";
    }
}
