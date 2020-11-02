package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.session.model.TopCategory;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by 小墨 on 2020/10/7 23:02
 */
public interface TopCategoryRepository extends JpaRepository<TopCategory, Long> {
}
