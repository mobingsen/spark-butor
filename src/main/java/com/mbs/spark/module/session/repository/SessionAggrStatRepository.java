package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.SessionAggrStat;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @Created by mobingsen on 2020/10/7 22:48
 */
public interface SessionAggrStatRepository extends JpaRepository<SessionAggrStat, Long> {
}
