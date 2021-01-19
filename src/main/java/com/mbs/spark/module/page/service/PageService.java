package com.mbs.spark.module.page.service;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.mbs.spark.conf.SparkConfig;
import com.mbs.spark.constant.Constants;
import com.mbs.spark.mock.MockData;
import com.mbs.spark.module.page.model.PageSliceRate;
import com.mbs.spark.module.page.repository.PageSliceRateRepository;
import com.mbs.spark.module.task.Param;
import com.mbs.spark.module.task.Task;
import com.mbs.spark.module.task.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 页面单跳转化率模块
 */
@Service
@RequiredArgsConstructor
public class PageService {

	private final PageSliceRateRepository pageSliceRateRepository;
	private final TaskRepository taskRepository;
	private final SparkConfig sparkConfig;

	public void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		if(sparkConfig.isLocal()) {
			conf.setMaster("local");
		}
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = sparkConfig.isLocal() ? new SQLContext(sc.sc()) : new HiveContext(sc.sc());
		if(sparkConfig.isLocal()) {
			MockData.mock(sc, sqlContext);
		}
		long taskid = 0;
		if(sparkConfig.isLocal()) {
			taskid = sparkConfig.getTaskPage();
		} else {
			try {
				if(args != null && args.length > 0) {
					taskid =  Long.parseLong(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Task task = taskRepository.findById(taskid).orElse(null);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		}

		// 查询指定日期范围内的用户访问行为数据
		String startDate = task.toParam().getStartDate();
		String endDate = task.toParam().getEndDate();
		String sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"
		/*
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行.所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
//		sqlContext.sql(sql).javaRDD().repartition(1000);
		JavaRDD<Row> actionRDD =  sqlContext.sql(sql).javaRDD();
		JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD
				// 获取<sessionid,用户访问行为>格式的数据
				.mapToPair(row -> new Tuple2<>(row.getString(2), row))
				.cache(); // persist(StorageLevel.MEMORY_ONLY)
		JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
		// 每个session的单跳页面切片的生成，以及页面流的匹配算法
		JavaPairRDD<String, Long> pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionsRDD, task.toParam());
		Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();
		// 使用者指定的页面流是3,2,5,8,6   现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6
		long startPagePv = getStartPagePv(task.toParam(), sessionid2actionsRDD);
		// 计算目标页面流的各个页面切片的转化率
		Map<String, Double> convertRateMap = computePageSplitConvertRate(task.toParam(), pageSplitPvMap, startPagePv);
		// 持久化页面切片转化率
		persistConvertRate(taskid, convertRateMap);
	}

	/**
	 * 页面切片生成与匹配算法
	 * @param sc
	 * @param sessionid2actionsRDD
	 * @param param
	 * @return
	 */
	private JavaPairRDD<String, Long> generateAndMatchPageSplit(JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD, Param param) {
		String targetPageFlow = param.getTargetPageFlow();
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		return sessionid2actionsRDD.flatMapToPair(tuple -> generateAndMatchPage(targetPageFlowBroadcast, tuple));
	}

	private Iterable<Tuple2<String, Long>> generateAndMatchPage(Broadcast<String> broadcast,
																		  Tuple2<String, Iterable<Row>> tuple) {
		String[] targetPages = broadcast.value().split(",");
		Comparator<Row> comparator = (o1, o2) -> {
			final DateTime dateTime1 = DateUtil.parse(o1.getString(4), DatePattern.NORM_DATETIME_PATTERN);
			final DateTime dateTime2 = DateUtil.parse(o2.getString(4), DatePattern.NORM_DATETIME_PATTERN);
			return dateTime1.compareTo(dateTime2);
		};
		List<Row> rows = StreamSupport.stream(tuple._2.spliterator(), false)
				.sorted(comparator)
				.collect(Collectors.toList());
		Long lastPageId = null;
		List<Tuple2<String, Long>> list = new ArrayList<>();
		for(Row row : rows) {
			long pageid = row.getLong(3);
			if(lastPageId == null) {
				lastPageId = pageid;
				continue;
			}
			// 生成一个页面切片
			// 3,5,2,1,8,9
			// lastPageId=3
			// 5，切片，3_5
			String pageSplit = lastPageId + "_" + pageid;
			// 对这个切片判断一下，是否在用户指定的页面流中
			for(int i = 1; i < targetPages.length; i++) {
				// 比如说，用户指定的页面流是3,2,5,8,1
				// 遍历的时候，从索引1开始，就是从第二个页面开始
				// 3_2
				String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
				if(pageSplit.equals(targetPageSplit)) {
					list.add(new Tuple2<>(pageSplit, 1L));
					break;
				}
			}
			lastPageId = pageid;
		}
		return list;
	}

	/**
	 * 获取页面流中初始页面的pv
	 * @param param
	 * @param sessionid2actionsRDD
	 * @return
	 */
	private long getStartPagePv(Param param, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
		String targetPageFlow = param.getTargetPageFlow();
		final long startPageId = Long.parseLong(targetPageFlow.split(",")[0]);
		BiFunction<Long, Tuple2<String, Iterable<Row>>, List<Row>> function = (startPageId1, t) -> StreamSupport
				.stream(t._2.spliterator(), false)
				.filter(row -> row.getLong(3) == startPageId1)
				.collect(Collectors.toList());
		return sessionid2actionsRDD.flatMap(tuple -> function.apply(startPageId, tuple)).count();
	}

	/**
	 * 计算页面切片转化率
	 *
	 * @param param
	 * @param pageSplitPvMap 页面切片pv
	 * @param startPagePv 起始页面pv
	 * @return
	 */
	private Map<String, Double> computePageSplitConvertRate(Param param, Map<String, Object> pageSplitPvMap, long startPagePv) {
		String[] targetPages = param.getTargetPageFlow().split(",");
		// 3,5,2,4,6
		// 3_5
		// 3_5 pv / 3 pv
		// 5_2 rate = 5_2 pv / 3_5 pv
		long lastPageSplitPv = 0L;
		Map<String, Double> convertRateMap = new HashMap<>();
		// 通过for循环，获取目标页面流中的各个页面切片（pv）
		for(int i = 1; i < targetPages.length; i++) {
			String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
			long targetPageSplitPv = Long.parseLong(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
			double convertRate = new BigDecimal(targetPageSplitPv)
					.divide(BigDecimal.valueOf(i == 1 ? startPagePv : lastPageSplitPv), 2, RoundingMode.HALF_UP)
					.doubleValue();
			convertRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPv = targetPageSplitPv;
		}
		return convertRateMap;
	}

	/**
	 * 持久化转化率
	 * @param convertRateMap
	 */
	private void persistConvertRate(long taskId, Map<String, Double> convertRateMap) {
		String convertRate = convertRateMap.entrySet().stream()
				.map(entry -> entry.getKey() + "=" + entry.getValue())
				.collect(Collectors.joining("|"));
		PageSliceRate pageSliceRate = new PageSliceRate();
		pageSliceRate.setTaskId(taskId);
		pageSliceRate.setRate(convertRate);
		pageSliceRateRepository.save(pageSliceRate);
	}

}
