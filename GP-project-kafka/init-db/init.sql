CREATE TABLE IF NOT EXISTS user_features (
    userId VARCHAR(255) NOT NULL,
    windowEnd TIMESTAMP NOT NULL,
    
    -- Auth
    login_fail_rate DOUBLE PRECISION,
    distinct_ip_count INT,
    
    -- Network
    api_request_count BIGINT,
    http_4xx_rate DOUBLE PRECISION,
    data_out_bytes BIGINT,
    api_path_entropy DOUBLE PRECISION,
    
    -- Security
    high_sev_alert_count BIGINT,
    avg_wazuh_level DOUBLE PRECISION,
    
    -- System
    avg_cpu_usage DOUBLE PRECISION,
    
    PRIMARY KEY (userId, windowEnd)
);

-- Trigger function to notify the ML model
CREATE OR REPLACE FUNCTION notify_model_trigger() RETURNS TRIGGER AS $$
DECLARE
    payload TEXT;
BEGIN
    payload := json_build_object(
        'userId', NEW.userId,
        'windowEnd', NEW.windowEnd
    )::text;
    PERFORM pg_notify('model_trigger', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_feature_insert ON user_features;
CREATE TRIGGER on_feature_insert
AFTER INSERT OR UPDATE ON user_features
FOR EACH ROW EXECUTE FUNCTION notify_model_trigger();