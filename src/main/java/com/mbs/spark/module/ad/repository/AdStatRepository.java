package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdStat;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @Created by mobingsen on 2020/10/7 18:02
 */
public interface AdStatRepository extends JpaRepository<AdStat, Long> {
}
