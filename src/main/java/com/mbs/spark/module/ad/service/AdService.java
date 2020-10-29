package com.mbs.spark.module.ad.service;

import com.google.common.base.Optional;
import com.mbs.spark.conf.KafkaConfigurer;
import com.mbs.spark.conf.SparkConfigurer;
import com.mbs.spark.module.ad.model.AdBlacklist;
import com.mbs.spark.module.ad.model.AdClickTrend;
import com.mbs.spark.module.ad.model.AdProvinceTop3;
import com.mbs.spark.module.ad.model.AdStat;
import com.mbs.spark.module.ad.model.AdUserClickCount;
import com.mbs.spark.module.ad.repository.AdBlackListRepository;
import com.mbs.spark.module.ad.repository.AdClickTrendRepository;
import com.mbs.spark.module.ad.repository.AdProvinceTopRepository;
import com.mbs.spark.module.ad.repository.AdStatRepository;
import com.mbs.spark.module.ad.repository.AdUserClickCountRepository;
import com.mbs.spark.tools.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 广告点击流量实时统计
 */
@Service
public class AdService {

    @Autowired
    SparkConfigurer sparkConfigurer;
    @Autowired
    KafkaConfigurer kafkaConfig;
    @Autowired
    AdBlackListRepository adBlackListRepository;
    @Autowired
    AdClickTrendRepository adClickTrendRepository;
    @Autowired
    AdProvinceTopRepository adProvinceTopRepository;
    @Autowired
    AdStatRepository adStatRepository;
    @Autowired
    AdUserClickCountRepository adUserClickCountRepository;

    public void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");
//				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//				.set("spark.default.parallelism", "1000");
//				.set("spark.streaming.blockInterval", "50");
//				.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        context.checkpoint("hdfs://192.168.1.105:9000/streaming_checkpoint");
        JavaPairInputDStream<String, String> inputDStream = KafkaUtils.createDirectStream(
                context,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaConfig.builderParams(),
                kafkaConfig.getTopicSet()
        );
//		inputDStream.repartition(1000);
        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = inputDStream
                .transformToPair(this::filterByBlacklist);
        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adId,clickCount）
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        calculateAdClickCountByWindow(inputDStream);

        context.start();
        context.awaitTermination();
        context.close();
    }

    /**
     * 根据黑名单进行过滤
     * @param rdd
     * @return
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private JavaPairRDD<String, String> filterByBlacklist(JavaPairRDD<String, String> rdd) {
        List<Tuple2<Long, Boolean>> tuples = adBlackListRepository.findAll().stream()
                .map(adBlacklist -> new Tuple2<>(adBlacklist.getUserId(), true))
                .collect(Collectors.toList());
        JavaSparkContext sc = new JavaSparkContext(rdd.context());
        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
        return rdd
                // 将原始数据rdd映射成<userId, tuple2<string, string>>
                .mapToPair(tuple -> new Tuple2<>(Long.parseLong(tuple._2.split(" ")[3]), tuple))
                // 将原始日志数据rdd，与黑名单rdd，进行左外连接 如果说原始日志的userId，没有在对应的黑名单中，join不到，左外连接
                .leftOuterJoin(blacklistRDD)
                // 如果这个值存在，那么说明原始日志中的userId，join到了某个黑名单用户
                .filter(tuple -> !(tuple._2._2.or(false)))
                .mapToPair(tuple -> tuple._2._1);
    }

    /**
     * 生成动态黑名单
     *
     * @param stream
     */
    private void generateDynamicBlacklist(JavaPairDStream<String, String> stream) {
        // timestamp province city userid adId
        // 某个时间点 某个省份 某个城市 某个用户 某个广告
        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = stream
                // 将日志的格式处理成<yyyyMMdd_userid_adId, 1L>格式
                .mapToPair(this::doDateUserIdAdIdAndOneLong)
                // （每个batch中）每天每个用户对每个广告的点击量
                .reduceByKey(Long::sum);
        // 每个5s的batch中当天每个用户对每支广告的点击次数<yyyyMMdd_userid_adId, clickCount>
        dailyUserAdClickCountDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(this::updateAdUserClickCount);
            return null;
        });
        dailyUserAdClickCountDStream
                .filter(this::checkClickCount)
                // yyyyMMdd_userid_adId,对其中的userid进行全局的去重
                .map(tuple -> Long.valueOf(tuple._1.split("_")[1]))
                .transform((Function<JavaRDD<Long>, JavaRDD<Long>>) JavaRDD::distinct)
                .foreachRDD(rdd -> {
                    rdd.foreachPartition(this::insertAdBlacklist);
                    return null;
                });
    }

    private void insertAdBlacklist(Iterator<Long> iterator) {
        List<AdBlacklist> blacklists = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(AdBlacklist::ctor).collect(Collectors.toList());
        adBlackListRepository.saveAll(blacklists);
    }

    private Boolean checkClickCount(Tuple2<String, Long> tuple) {
        String key = tuple._1;
        String[] keyArr = key.split("_");
        // yyyyMMdd -> yyyy-MM-dd
        String date = DateUtils.formatDate(DateUtils.parseDateKey(keyArr[0]));
        long userId = Long.parseLong(keyArr[1]);
        long adId = Long.parseLong(keyArr[2]);
        // 从mysql中查询指定日期指定用户对指定广告的点击量
        int clickCount = adUserClickCountRepository.findClickCountByMultiKey(date, userId, adId);
        // 点击量大于等于100是黑名单用户;反之，小于100的，那就不要管了
        return clickCount >= 100;
    }

    private void updateAdUserClickCount(Iterator<Tuple2<String, Long>> iterator) {
        List<AdUserClickCount> adUserClickCounts = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(tuple -> {
                    String[] tupleKey = tuple._1.split("_");
                    String date = DateUtils.formatDate(DateUtils.parseDateKey(tupleKey[0]));
                    // yyyy-MM-dd
                    long userId = Long.parseLong(tupleKey[1]);
                    long adId = Long.parseLong(tupleKey[2]);
                    long clickCount = tuple._2;
                    AdUserClickCount adUserClickCount = new AdUserClickCount();
                    adUserClickCount.setDate(date);
                    adUserClickCount.setUserId(userId);
                    adUserClickCount.setAdId(adId);
                    adUserClickCount.setClickCount(clickCount);
                    return adUserClickCount;
                }).collect(Collectors.toList());
        adUserClickCountRepository.saveAll(adUserClickCounts);
    }

    private Tuple2<String, Long> doDateUserIdAdIdAndOneLong(Tuple2<String, String> tuple) {
        String log = tuple._2;
        String[] logArr = log.split(" ");
        // 提取出日期（yyyyMMdd）、userId、adId
        String timestamp = logArr[0];
        Date date = new Date(Long.parseLong(timestamp));
        String dateKey = DateUtils.formatDateKey(date);
        long userId = Long.parseLong(logArr[3]);
        long adId = Long.parseLong(logArr[4]);
        return new Tuple2<>(dateKey + "_" + userId + "_" + adId, 1L);
    }

    /**
     * 计算广告点击流量实时统计
     *
     * @param javaPairDStream
     * @return
     */
    private JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> javaPairDStream) {
        // 对原始数据进行map，映射成<date_province_city_adId,1>格式
        JavaPairDStream<String, Long> aggregatedDStream = javaPairDStream
                .mapToPair(this::handleAdRealTimeLog)
                .updateStateByKey(this::doNewestValue);
        aggregatedDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(this::updateAdStat);
            return null;
        });
        return aggregatedDStream;
    }

    private Tuple2<String, Long> handleAdRealTimeLog(Tuple2<String, String> tuple) {
        String log = tuple._2;
        String[] logArr = log.split(" ");
        String timestamp = logArr[0];
        Date date = new Date(Long.parseLong(timestamp));
        String dateKey = DateUtils.formatDateKey(date);    // yyyyMMdd
        String province = logArr[1];
        String city = logArr[2];
        long adId = Long.parseLong(logArr[4]);
        return new Tuple2<>(dateKey + "_" + province + "_" + city + "_" + adId, 1L);
    }

    @SuppressWarnings({"Guava", "OptionalUsedAsFieldOrParameterType"})
    private Optional<Long> doNewestValue(List<Long> values, Optional<Long> optional) {
        Long reduce = values.stream().reduce(optional.or(0L), Long::sum);
        return Optional.of(reduce);
    }

    private void updateAdStat(Iterator<Tuple2<String, Long>> iterator) {
        List<AdStat> stats = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(AdStat::ctor).collect(Collectors.toList());
        adStatRepository.saveAll(stats);
    }

    /**
     * 计算每天各省份的top3热门广告
     *
     * @param stream
     */
    private void calculateProvinceTop3Ad(JavaPairDStream<String, Long> stream) {
        stream.transform(this::calculateProvinceTop)
                .foreachRDD(rdd -> {
                    rdd.foreachPartition(this::updateAdProvinceTop);
                    return null;
                });
    }

    private void updateAdProvinceTop(Iterator<Row> iterator) {
        List<AdProvinceTop3> tops = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(row -> {
                    String date = row.getString(0);
                    String province = row.getString(1);
                    long adId = row.getLong(2);
                    long clickCount = row.getLong(3);
                    AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                    adProvinceTop3.setDate(date);
                    adProvinceTop3.setProvince(province);
                    adProvinceTop3.setAdId(adId);
                    adProvinceTop3.setClickCount(clickCount);
                    return adProvinceTop3;
                }).collect(Collectors.toList());
        tops.stream().map(top -> top.getDate() + "_" + top.getProvince()).distinct()
                .forEach(str -> adProvinceTopRepository.deleteByDateAndProvince(str.split("_")[0], str.split("_")[1]));
        // 批量插入传入进来的所有数据
        adProvinceTopRepository.saveAll(tops);
    }

    private JavaRDD<Row> calculateProvinceTop(JavaPairRDD<String, Long> rdd) {
        JavaRDD<Row> javaRDD = rdd
                // 计算出每天各省份各广告的点击量 <yyyyMMdd_province_city_adId, clickCount> ==> <yyyyMMdd_province_adId, clickCount>
                .mapToPair(this::calculatePerDayClickCount)
                .reduceByKey(Long::sum)
                // 获取到各省份的top3热门广告
                .map(this::getDailyAdClickCountRow);
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("province", DataTypes.StringType, true),
                DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_count", DataTypes.LongType, true)));
        HiveContext sqlContext = new HiveContext(rdd.context());
        sqlContext.createDataFrame(javaRDD, schema)
                .registerTempTable("tmp_daily_ad_click_count_by_prov");
        return sqlContext
                .sql(
                        "SELECT "
                                + "date,"
                                + "province,"
                                + "ad_id,"
                                + "click_count "
                                + "FROM ( "
                                + "SELECT "
                                + "date,"
                                + "province,"
                                + "ad_id,"
                                + "click_count,"
                                + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
                                + "FROM tmp_daily_ad_click_count_by_prov "
                                + ") t "
                                + "WHERE rank>=3"
                ).javaRDD();
    }

    private Row getDailyAdClickCountRow(Tuple2<String, Long> tuple) {
        String[] keyArr = tuple._1.split("_");
        String date = DateUtils.formatDate(DateUtils.parseDateKey(keyArr[0]));
        String province = keyArr[1];
        long adId = Long.parseLong(keyArr[2]);
        long clickCount = tuple._2;
        return RowFactory.create(date, province, adId, clickCount);
    }

    private Tuple2<String, Long> calculatePerDayClickCount(Tuple2<String, Long> tuple) {
        String[] keyArr = tuple._1.split("_");
        String date = keyArr[0];
        String province = keyArr[1];
        long adId = Long.parseLong(keyArr[3]);
        long clickCount = tuple._2;
        return new Tuple2<>(date + "_" + province + "_" + adId, clickCount);
    }

    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     *
     * @param adRealTimeLogDStream
     */
    private void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        adRealTimeLogDStream
                // 映射成<yyyyMMddHHMM_adId,1L>格式
                .mapToPair(this::doDateAdIdAndOneLong)
                // 统计出来最近一小时内的各分钟各广告的点击次数
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(60), Durations.seconds(10))
                .foreachRDD(rdd -> {
                    rdd.foreachPartition(this::updateAdClickTrend);
                    return null;
                });
    }

    private Tuple2<String, Long> doDateAdIdAndOneLong(Tuple2<String, String> tuple) {
        // timestamp province city userId adId
        String[] logArr = tuple._2.split(" ");
        String timeMinute = DateUtils.formatTimeMinute(new Date(Long.parseLong(logArr[0])));
        long adId = Long.parseLong(logArr[4]);
        return new Tuple2<>(timeMinute + "_" + adId, 1L);
    }

    private void updateAdClickTrend(Iterator<Tuple2<String, Long>> iterator) {
        List<AdClickTrend> adClickTrends = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(tuple -> {
                    String[] keyArr = tuple._1.split("_");
                    // yyyyMMddHHmm
                    String dateMinute = keyArr[0];
                    long adId = Long.parseLong(keyArr[1]);
                    long clickCount = tuple._2;
                    String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                    String hour = dateMinute.substring(8, 10);
                    String minute = dateMinute.substring(10);
                    AdClickTrend adClickTrend = new AdClickTrend();
                    adClickTrend.setDate(date);
                    adClickTrend.setHour(hour);
                    adClickTrend.setMinute(minute);
                    adClickTrend.setAdId(adId);
                    adClickTrend.setClickCount(clickCount);
                    return adClickTrend;
                }).collect(Collectors.toList());
        adClickTrendRepository.saveAll(adClickTrends);
    }
}
