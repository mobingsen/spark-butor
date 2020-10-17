package com.mbs.spark.module.page.service;

import com.mbs.spark.conf.SparkConfigurer;
import com.mbs.spark.constant.Constants;
import com.mbs.spark.module.page.model.PageSplitConvertRate;
import com.mbs.spark.module.page.repository.PageSplitConvertRateRepository;
import com.mbs.spark.module.task.model.Param;
import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.repository.TaskRepository;
import com.mbs.spark.test.MockData;
import com.mbs.spark.util.DateUtils;
import com.mbs.spark.util.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 页面单跳转化率模块
 */
@Service
public class PageOneStepConvertRateService {

	@Autowired
	PageSplitConvertRateRepository pageSplitConvertRateRepository;
	@Autowired
	TaskRepository taskRepository;
	@Autowired
	SparkConfigurer sparkConfigurer;

	public void main(String[] args) {
		// 1、构造Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		if(sparkConfigurer.isLocal()) {
			conf.setMaster("local");
		}
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = sparkConfigurer.isLocal() ? new SQLContext(sc.sc()) : new HiveContext(sc.sc());

		// 2、生成模拟数据
		if(sparkConfigurer.isLocal()) {
			MockData.mock(sc, sqlContext);
		}
		// 3、查询任务，获取任务的参数
		long taskid = 0;
		if(sparkConfigurer.isLocal()) {
			taskid = sparkConfigurer.getTaskPage();
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

		// 4、查询指定日期范围内的用户访问行为数据
		String startDate = task.toParam().getStartDate();
		String endDate = task.toParam().getEndDate();
		String sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"
		/*
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 *
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
//		sqlContext.sql(sql).javaRDD().repartition(1000);
		JavaRDD<Row> actionRDD =  sqlContext.sql(sql).javaRDD();
		// 对用户访问行为数据做一个映射，将其映射为<sessionid,访问行为>的格式
		// 咱们的用户访问页面切片的生成，是要基于每个session的访问数据，来进行生成的
		// 脱离了session，生成的页面访问切片，是么有意义的
		// 举例，比如用户A，访问了页面3和页面5
		// 用于B，访问了页面4和页面6
		// 漏了一个前提，使用者指定的页面流筛选条件，比如页面3->页面4->页面7
		// 你能不能说，是将页面3->页面4，串起来，作为一个页面切片，来进行统计呢
		// 当然不行
		// 所以说呢，页面切片的生成，肯定是要基于用户session粒度的

		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
		sessionid2actionRDD = sessionid2actionRDD.cache(); // persist(StorageLevel.MEMORY_ONLY)

		// 对<sessionid,访问行为> RDD，做一次groupByKey操作
		// 因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
		JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();

		// 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
		JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionsRDD, task.toParam());
		Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

		// 使用者指定的页面流是3,2,5,8,6
		// 咱们现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6
		long startPagePv = getStartPagePv(task.toParam(), sessionid2actionsRDD);

		// 计算目标页面流的各个页面切片的转化率
		Map<String, Double> convertRateMap = computePageSplitConvertRate(task.toParam(), pageSplitPvMap, startPagePv);

		// 持久化页面切片转化率
		persistConvertRate(taskid, convertRateMap);
	}

	/**
	 * 获取指定日期范围内的用户行为数据RDD
	 * @param sqlContext
	 * @param param
	 * @return
	 */
	public JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, Param param) {
		String startDate = param.getStartDate();
		String endDate = param.getEndDate();
		String sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"
		/*
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 *
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
//		return sqlContext.sql(sql).javaRDD().repartition(1000);
		return sqlContext.sql(sql).javaRDD();
	}

	/**
	 * 获取<sessionid,用户访问行为>格式的数据
	 * @param actionRDD 用户访问行为RDD
	 * @return <sessionid,用户访问行为>格式的数据
	 */
	private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2), row));
	}

	/**
	 * 页面切片生成与匹配算法
	 * @param sc
	 * @param sessionid2actionsRDD
	 * @param param
	 * @return
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD, Param param) {
		String targetPageFlow = param.getTargetPageFlow();
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		return sessionid2actionsRDD.flatMapToPair(tuple -> generateAndMatchPage(targetPageFlowBroadcast, tuple));
	}

	private static Iterable<Tuple2<String, Integer>> generateAndMatchPage(Broadcast<String> targetPageFlowBroadcast,
																		  Tuple2<String, Iterable<Row>> tuple) {
		List<Tuple2<String, Integer>> list = new ArrayList<>();
		// 获取到当前session的访问行为的迭代器
		// 获取使用者指定的页面流
		// 使用者指定的页面流，1,2,3,4,5,6,7
		// 1->2的转化率是多少？2->3的转化率是多少？
		String[] targetPages = targetPageFlowBroadcast.value().split(",");
		// 这里，我们拿到的session的访问行为，默认情况下是乱序的
		// 比如说，正常情况下，我们希望拿到的数据，是按照时间顺序排序的
		// 但是问题是，默认是不排序的
		// 所以，我们第一件事情，对session的访问行为数据按照时间进行排序

		// 举例，反例
		// 比如，3->5->4->10->7
		// 3->4->5->7->10
		// 排序
		Comparator<Row> comparator = (o1, o2) -> {
			Date date1 = DateUtils.parseTime(o1.getString(4));
			Date date2 = DateUtils.parseTime(o2.getString(4));
			assert date1 != null;
			assert date2 != null;
			return (int) (date1.getTime() - date2.getTime());
		};
		List<Row> rows = StreamSupport.stream(tuple._2.spliterator(), false)
				.sorted(comparator)
				.collect(Collectors.toList());
		// 页面切片的生成，以及页面流的匹配
		Long lastPageId = null;
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
					list.add(new Tuple2<>(pageSplit, 1));
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
	private static long getStartPagePv(Param param, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
		String targetPageFlow = param.getTargetPageFlow();
		final long startPageId = Long.parseLong(targetPageFlow.split(",")[0]);
		return sessionid2actionsRDD
				.flatMap(tuple -> StreamSupport.stream(tuple._2.spliterator(), true)
						.filter(row -> row.getLong(3) == startPageId).collect(Collectors.toList())
				).count();
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
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskId(taskId);
		pageSplitConvertRate.setConvertRate(convertRate);
		pageSplitConvertRateRepository.save(pageSplitConvertRate);
	}

}
