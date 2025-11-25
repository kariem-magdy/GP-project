CREATE TABLE IF NOT EXISTS user_features (
    userId VARCHAR(255) NOT NULL,
    windowEnd TIMESTAMP NOT NULL,
    login_fail_rate DOUBLE PRECISION,
    distinct_ip_count INT,
    api_request_count BIGINT,
    http_4xx_rate DOUBLE PRECISION,
    data_out_bytes BIGINT,
    api_path_entropy DOUBLE PRECISION,
    high_sev_alert_count BIGINT,
    avg_wazuh_level DOUBLE PRECISION,
    avg_cpu_usage DOUBLE PRECISION,
    PRIMARY KEY (userId, windowEnd)
);

-- Trigger Function
CREATE OR REPLACE FUNCTION notify_model_trigger() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('model_trigger', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER on_feature_insert
AFTER INSERT OR UPDATE ON user_features
FOR EACH ROW EXECUTE FUNCTION notify_model_trigger();