package org.phoebus.applications.alarm.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "alarm.server")
public class AlarmServerProperties {

    private String kafkaServer = "localhost:9092";
    private String config = "Accelerator";
    private String kafkaProperties = "";
    private boolean shellEnabled = false;
    private long connectionSecs = 10;
    private long stableSecs = 4;

    public String getKafkaServer() {
        return kafkaServer;
    }

    public void setKafkaServer(String kafkaServer) {
        this.kafkaServer = kafkaServer;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getKafkaProperties() {
        return kafkaProperties;
    }

    public void setKafkaProperties(String kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public boolean isShellEnabled() {
        return shellEnabled;
    }

    public void setShellEnabled(boolean shellEnabled) {
        this.shellEnabled = shellEnabled;
    }

    public long getConnectionSecs() {
        return connectionSecs;
    }

    public void setConnectionSecs(long connectionSecs) {
        this.connectionSecs = connectionSecs;
    }

    public long getStableSecs() {
        return stableSecs;
    }

    public void setStableSecs(long stableSecs) {
        this.stableSecs = stableSecs;
    }
}