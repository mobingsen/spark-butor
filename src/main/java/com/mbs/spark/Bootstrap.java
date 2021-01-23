package com.mbs.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.reactive.config.EnableWebFlux;

/**
 * @Created by mobingsen on 2020/10/7 12:49
 */
@SpringBootApplication
@EnableScheduling
@EnableWebFlux
@EnableTransactionManagement
@EnableAspectJAutoProxy
public class Bootstrap {

    public static void main(String[] args) {
        SpringApplication.run(Bootstrap.class);
    }
}
