package com.example;

import java.io.Serializable;

public abstract class BaseEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public String userId;       // The JOIN key across all sources
    public long timestamp;      // Event time in epoch millis
    public String eventType;    // Discriminator (KEYCLOAK, HAPROXY, WAZUH, PROMETHEUS)

    public BaseEvent() {}
}
