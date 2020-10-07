package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdProvinceTop3;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * Created by 小墨 on 2020/10/7 17:53
 */
public interface AdProvinceTopRepository extends JpaRepository<AdProvinceTop3, Long> {

    @Query("DELETE FROM AdProvinceTop3 WHERE date=?1 AND province=?2")
    void deleteByDateAndProvince(String date, String province);
}
