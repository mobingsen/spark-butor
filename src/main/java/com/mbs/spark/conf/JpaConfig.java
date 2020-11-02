package com.mbs.spark.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories("com.mbs.spark.*.**")
@EnableJpaAuditing
public class JpaConfig {
}
