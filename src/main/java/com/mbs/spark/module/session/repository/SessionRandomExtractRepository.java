package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.SessionRandomExtract;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @Created by mobingsen on 2020/10/7 22:54
 */
public interface SessionRandomExtractRepository extends JpaRepository<SessionRandomExtract, Long> {
}
