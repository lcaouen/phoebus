package org.phoebus.applications.alarm.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AlarmServerProperties.class)
public class AlarmServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlarmServerApplication.class, args);
    }
}