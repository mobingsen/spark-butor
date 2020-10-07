package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.SessionDetail;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by 小墨 on 2020/10/7 22:50
 */
public interface SessionDetailRepository extends JpaRepository<SessionDetail, Long> {
}
