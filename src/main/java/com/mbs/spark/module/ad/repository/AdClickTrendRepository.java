package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdClickTrend;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/**
 * Created by 小墨 on 2020/10/7 15:21
 */
@Repository
public interface AdClickTrendRepository extends JpaRepository<AdClickTrend, Long> {

    @Query("SELECT count(*) FROM ad_click_trend WHERE date=?1 AND hour=?2 AND minute=?3 AND ad_id=?4")
    int getAdClickTrend(String date, String hour, String minute, long adId);
}
