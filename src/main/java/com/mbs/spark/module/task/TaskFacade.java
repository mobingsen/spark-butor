package com.mbs.spark.module.task;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Component
@RequiredArgsConstructor
public class TaskFacade {

    private final TaskService taskService;

    public Mono<ServerResponse> add(ServerRequest request) {
        taskService.add();
        return ServerResponse
                .ok()
                .build();
    }

    public Mono<ServerResponse> findAll(ServerRequest request) {
        List<Task> all = taskService.all();
        return ServerResponse
                .ok()
                .contentType(APPLICATION_JSON)
                .body(all, Task.class);
    }
}
