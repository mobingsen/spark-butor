package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.TopSession;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @Created by mobingsen on 2020/10/7 23:04
 */
public interface TopSessionRepository extends JpaRepository<TopSession, Long> {
}
