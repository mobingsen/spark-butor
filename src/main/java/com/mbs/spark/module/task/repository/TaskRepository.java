package com.mbs.spark.module.task.repository;

import com.mbs.spark.module.task.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @Created by mobingsen on 2020/10/7 22:57
 */
public interface TaskRepository extends JpaRepository<Task, Long> {
}
