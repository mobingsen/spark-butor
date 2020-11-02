package com.mbs.spark.conf;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by 小墨 on 2020/10/7 0:24
 */
@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "spark")
public class SparkConfig {

    private boolean local;
    private int taskSession;
    private int taskPage;
    private int taskProduct;
}
