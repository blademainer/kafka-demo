package com.xiongyingqi.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "spring.kafka")
public class ZookeeperConfig {
    private String dataDir;
    private String port;
}
