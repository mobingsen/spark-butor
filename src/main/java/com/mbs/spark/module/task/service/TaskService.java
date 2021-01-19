package com.mbs.spark.module.task.service;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.ImmutableMap;
import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.repository.TaskRepository;
import com.mbs.spark.module.task.vo.TaskRunVo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;
    private final static int processors = Runtime.getRuntime().availableProcessors();
    private final static AtomicInteger executorId = new AtomicInteger(0);
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(processors, processors, 30L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(500),
            r -> new Thread(r, "TASK THREAD POOL " + executorId.incrementAndGet()),
            (r, executor) -> log.info("task is overload"));

    public void add() {
        Task task = new Task();
        task.setArgs(ImmutableMap.of("1", "2"));
        taskRepository.save(task);
    }

    public List<Task> all() {
        return taskRepository.findAll();
    }

    public void doTask(TaskRunVo taskRunVo) {
        Task task = new Task()
                .setTaskName(taskRunVo.getTaskName())
                .setTaskType(taskRunVo.getType())
                .setArgs(taskRunVo.getArgs())
                .setCreateTime(DateUtil.now());
        final Task save = taskRepository.save(task);
        executor.submit(() -> handle(save));
    }

    public void handle(Task task) {
        final String now = DateUtil.now();
        task.setStartTime(now);
        final String taskType = task.getTaskType();

        taskRepository.save(task);
    }
}
