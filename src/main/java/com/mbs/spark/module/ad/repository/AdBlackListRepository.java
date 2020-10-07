package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdBlacklist;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by 小墨 on 2020/10/7 14:42
 */
@Repository
public interface AdBlackListRepository extends JpaRepository<AdBlacklist, Long> {
}
