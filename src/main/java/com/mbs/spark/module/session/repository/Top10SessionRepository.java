package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.Top10Session;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by 小墨 on 2020/10/7 23:04
 */
public interface Top10SessionRepository extends JpaRepository<Top10Session, Long> {
}
