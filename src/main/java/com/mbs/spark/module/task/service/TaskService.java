package com.mbs.spark.module.task.service;

import com.google.common.collect.ImmutableMap;
import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.repository.TaskRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TaskService {

    @Autowired
    TaskRepository taskRepository;

    public void add() {
        Task task = new Task();
        task.setArgs(ImmutableMap.of("1", "2"));
        taskRepository.save(task);
    }

    public List<Task> all() {
        return taskRepository.findAll();
    }
}
