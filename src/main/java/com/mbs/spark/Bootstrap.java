package com.mbs.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Created by 小墨 on 2020/10/7 12:49
 */
@SpringBootApplication
@EnableScheduling
@EnableTransactionManagement
public class Bootstrap {

    public static void main(String[] args) {
        SpringApplication.run(Bootstrap.class);
    }
}
