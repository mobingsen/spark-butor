package com.mbs.spark.module.task;

import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;

    public void add() {
        Task task = new Task();
        task.setArgs(ImmutableMap.of("1", "2"));
        taskRepository.save(task);
    }

    public List<Task> all() {
        return taskRepository.findAll();
    }
}
