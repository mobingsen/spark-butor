package com.mbs.spark.module.session.repository;

import com.mbs.spark.module.product.model.Top10Category;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by 小墨 on 2020/10/7 23:02
 */
public interface Top10CategoryRepository extends JpaRepository<Top10Category, Long> {
}
