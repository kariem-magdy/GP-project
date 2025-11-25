package com.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

public class JdbcUserFeaturesSink extends RichSinkFunction<UserFeatures> {
    private Connection connection;
    private PreparedStatement ps;
    private Statement notifyStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Opening JDBC connection to PostgreSQL...");
        
        // Load the PostgreSQL driver explicitly
        Class.forName("org.postgresql.Driver");
        
        // Connection with retry logic
        int maxRetries = 5;
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < maxRetries) {
            try {
                connection = DriverManager.getConnection(
                    "jdbc:postgresql://postgres:5432/ueba", 
                    "flink", 
                    "flink"
                );
                System.out.println("Successfully connected to PostgreSQL!");
                break;
            } catch (Exception e) {
                lastException = e;
                retryCount++;
                System.err.println("Failed to connect to PostgreSQL (attempt " + retryCount + "/" + maxRetries + "): " + e.getMessage());
                if (retryCount < maxRetries) {
                    Thread.sleep(5000); // Wait 5 seconds before retry
                }
            }
        }
        
        if (connection == null) {
            throw new RuntimeException("Failed to connect to PostgreSQL after " + maxRetries + " attempts", lastException);
        }
        
        // LOGIC NOTE: We use ON CONFLICT to handle re-calculations or duplicate window firings
        String sql = "INSERT INTO user_features (userId, windowEnd, login_fail_rate, distinct_ip_count, " +
                     "api_request_count, http_4xx_rate, data_out_bytes, api_path_entropy, " +
                     "high_sev_alert_count, avg_wazuh_level, avg_cpu_usage) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                     "ON CONFLICT (userId, windowEnd) DO UPDATE SET " +
                     "login_fail_rate = EXCLUDED.login_fail_rate, " +
                     "distinct_ip_count = EXCLUDED.distinct_ip_count, " +
                     "api_request_count = EXCLUDED.api_request_count, " +
                     "http_4xx_rate = EXCLUDED.http_4xx_rate, " +
                     "data_out_bytes = EXCLUDED.data_out_bytes, " +
                     "api_path_entropy = EXCLUDED.api_path_entropy, " +
                     "high_sev_alert_count = EXCLUDED.high_sev_alert_count, " +
                     "avg_wazuh_level = EXCLUDED.avg_wazuh_level, " +
                     "avg_cpu_usage = EXCLUDED.avg_cpu_usage";
        ps = connection.prepareStatement(sql);
        notifyStmt = connection.createStatement();
    }

    @Override
    public void invoke(UserFeatures value, Context context) throws Exception {
        try {
            System.out.println("Inserting feature for user: " + value.userId + " at window: " + new Timestamp(value.windowEnd));
            
            ps.setString(1, value.userId);
            ps.setTimestamp(2, new Timestamp(value.windowEnd));
            ps.setDouble(3, value.login_fail_rate);
            ps.setInt(4, value.distinct_ip_count);
            ps.setLong(5, value.api_request_count);
            ps.setDouble(6, value.http_4xx_rate);
            ps.setLong(7, value.data_out_bytes);
            ps.setDouble(8, value.api_path_entropy);
            ps.setLong(9, value.high_sev_alert_count);
            ps.setDouble(10, value.avg_wazuh_level);
            ps.setDouble(11, value.avg_cpu_usage);
            
            int rowsAffected = ps.executeUpdate();
            System.out.println("Successfully inserted/updated " + rowsAffected + " rows for user: " + value.userId);

            // FIRE TRIGGER
            String payload = String.format("{\"userId\": \"%s\", \"windowEnd\": %d}", value.userId, value.windowEnd);
            notifyStmt.execute("NOTIFY model_trigger, '" + payload + "'");
            System.out.println("Sent notification for user: " + value.userId);
            
        } catch (Exception e) {
            System.err.println("Error inserting user features: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("Closing JDBC connection...");
        if (ps != null) ps.close();
        if (notifyStmt != null) notifyStmt.close();
        if (connection != null) connection.close();
    }
}