package com.mbs.spark.conf;

import com.mbs.spark.module.task.facade.TaskFacade;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;

@Configuration
public class RouterConfig {

    @Bean
    public RouterFunction<ServerResponse> router(TaskFacade taskFacade) {
        return RouterFunctions
                .route(GET("/findAll"), taskFacade::findAll)
                .andRoute(GET("/add"), taskFacade::add)
                ;
    }
}
