package com.mbs.spark.module.task;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by 小墨 on 2020/10/7 22:57
 */
public interface TaskRepository extends JpaRepository<Task, Long> {
}