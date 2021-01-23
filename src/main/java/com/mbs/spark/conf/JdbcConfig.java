package com.mbs.spark.conf;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Created by mobingsen on 2020/10/8 10:25
 */
@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "spring.datasource")
public class JdbcConfig {

    private String url;
    private String username;
    private String password;
}
