package com.example;

public class HaproxyEvent extends BaseEvent {
    private static final long serialVersionUID = 1L;

    public int status;         // HTTP status code
    public String method;      // HTTP method (GET, POST, etc.)
    public String path;        // Request path
    public long bytes;         // Bytes read
    public String userAgent;   // User agent string

    public HaproxyEvent() {
        this.eventType = "HAPROXY";
    }
}
