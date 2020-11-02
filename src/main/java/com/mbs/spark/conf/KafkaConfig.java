package com.mbs.spark.conf;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by 小墨 on 2020/10/7 0:24
 */
@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {

    private String brokers;
    private String topics;

    public Set<String> getTopicSet() {
        return Arrays.stream(this.topics.split(",")).collect(Collectors.toSet());
    }

    public Map<String, String> builderParams() {
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("kafka.metadata.broker.list", getBrokers());
        return kafkaParams;
    }
}
