package com.xiongyingqi.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author xiongyingqi
 * @since 20171214//
 */
@Data
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfig extends KafkaProperties {
    private String port;
    private String logDir;
}
