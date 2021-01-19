package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdClickTrend;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/**
 * Created by 小墨 on 2020/10/7 15:21
 */
public interface AdClickTrendRepository extends JpaRepository<AdClickTrend, Long> {

    @Query("SELECT count(act) FROM tb_ad_click_trend act WHERE act.date=?1 AND act.hour=?2 AND act.minute=?3 AND act.adId=?4")
    int getAdClickTrend(String date, String hour, String minute, long adId);
}
