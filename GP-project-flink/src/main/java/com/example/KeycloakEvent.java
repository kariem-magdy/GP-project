package com.example;

public class KeycloakEvent extends BaseEvent {
    private static final long serialVersionUID = 1L;

    public String type;       // LOGIN, LOGIN_ERROR, REFRESH_TOKEN, LOGOUT
    public String ip;         // Source IP address
    public String clientId;   // OAuth client ID

    public KeycloakEvent() {
        this.eventType = "KEYCLOAK";
    }
}
