package com.mbs.spark.module.page.service;

import com.mbs.spark.constant.Constants;
import com.mbs.spark.module.page.model.PageSplitConvertRate;
import com.mbs.spark.module.page.repository.PageSplitConvertRateRepository;
import com.mbs.spark.module.task.model.Param;
import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.repository.TaskRepository;
import com.mbs.spark.util.DateUtils;
import com.mbs.spark.util.NumberUtils;
import com.mbs.spark.util.ParamUtils;
import com.mbs.spark.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

	public void main(String[] args) {
		// 1、构造Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

		// 2、生成模拟数据
		SparkUtils.mockData(sc, sqlContext);

		// 3、查询任务，获取任务的参数
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

		Task task = taskRepository.findById(taskid).orElse(null);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		}

		// 4、查询指定日期范围内的用户访问行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, task.toParam());

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
	 * 获取<sessionid,用户访问行为>格式的数据
	 * @param actionRDD 用户访问行为RDD
	 * @return <sessionid,用户访问行为>格式的数据
	 */
	private static JavaPairRDD<String, Row> getSessionid2actionRDD(
			JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2), row));
	}

	/**
	 * 页面切片生成与匹配算法
	 * @param sc
	 * @param sessionid2actionsRDD
	 * @param param
	 * @return
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
			JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
			Param param) {
		String targetPageFlow = param.getTargetPageFlow();
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		return sessionid2actionsRDD.flatMapToPair(tuple -> generateAndMatchPage(targetPageFlowBroadcast, tuple));
	}

	private static Iterable<Tuple2<String, Integer>> generateAndMatchPage(Broadcast<String> targetPageFlowBroadcast, Tuple2<String, Iterable<Row>> tuple) {
		// 定义返回list
		List<Tuple2<String, Integer>> list = new ArrayList<>();
		// 获取到当前session的访问行为的迭代器
		Iterator<Row> iterator = tuple._2.iterator();
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

		List<Row> rows = new ArrayList<Row>();
		while(iterator.hasNext()) {
			rows.add(iterator.next());
		}

		rows.sort((o1, o2) -> {
			String actionTime1 = o1.getString(4);
			String actionTime2 = o2.getString(4);

			Date date1 = DateUtils.parseTime(actionTime1);
			Date date2 = DateUtils.parseTime(actionTime2);

			return (int) (date1.getTime() - date2.getTime());
		});

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
					list.add(new Tuple2<String, Integer>(pageSplit, 1));
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
	private static long getStartPagePv(Param param,
									   JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
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
	private static Map<String, Double> computePageSplitConvertRate(
			Param param,
			Map<String, Object> pageSplitPvMap,
			long startPagePv) {
		Map<String, Double> convertRateMap = new HashMap<>();

		String[] targetPages = param.getTargetPageFlow().split(",");

		long lastPageSplitPv = 0L;

		// 3,5,2,4,6
		// 3_5
		// 3_5 pv / 3 pv
		// 5_2 rate = 5_2 pv / 3_5 pv

		// 通过for循环，获取目标页面流中的各个页面切片（pv）
		for(int i = 1; i < targetPages.length; i++) {
			String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
			long targetPageSplitPv = Long.parseLong(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

			double convertRate = 0.0;

			if(i == 1) {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)startPagePv, 2);
			} else {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)lastPageSplitPv, 2);
			}

			convertRateMap.put(targetPageSplit, convertRate);

			lastPageSplitPv = targetPageSplitPv;
		}

		return convertRateMap;
	}

	/**
	 * 持久化转化率
	 * @param convertRateMap
	 */
	private void persistConvertRate(long taskid, Map<String, Double> convertRateMap) {
		StringBuilder buffer = new StringBuilder("");

		for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
			String pageSplit = convertRateEntry.getKey();
			double convertRate = convertRateEntry.getValue();
			buffer.append(pageSplit).append("=").append(convertRate).append("|");
		}

		String convertRate = buffer.toString();
		convertRate = convertRate.substring(0, convertRate.length() - 1);

		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskId(taskid);
		pageSplitConvertRate.setConvertRate(convertRate);
		pageSplitConvertRateRepository.save(pageSplitConvertRate);
	}

}
