package com.mbs.spark.module.session.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.gson.Gson;
import com.mbs.spark.conf.SparkConfig;
import com.mbs.spark.constant.Constants;
import com.mbs.spark.mock.MockData;
import com.mbs.spark.module.session.CategorySortKey;
import com.mbs.spark.module.session.SessionAggrStatAccumulator;
import com.mbs.spark.module.session.model.*;
import com.mbs.spark.module.session.repository.*;
import com.mbs.spark.module.task.Param;
import com.mbs.spark.module.task.Task;
import com.mbs.spark.module.task.TaskRepository;
import com.mbs.spark.tools.DateUtils;
import com.mbs.spark.tools.ValidUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
@RequiredArgsConstructor
public class SessionService {

	private final SessionAggrStatRepository sessionAggrStatRepository;
	private final SessionDetailRepository sessionDetailRepository;
	private final SessionRandomExtractRepository sessionRandomExtractRepository;
	private final TaskRepository taskRepository;
	private final TopCategoryRepository topCategoryRepository;
	private final TopSessionRepository topSessionRepository;
	private final SparkConfig sparkConfig;

	public void main(String[] args) {
		// 构建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
//				.set("spark.default.parallelism", "100")
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.reducer.maxSizeInFlight", "24")
				.set("spark.shuffle.io.maxRetries", "60")
				.set("spark.shuffle.io.retryWait", "60")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{CategorySortKey.class, IntList.class});
		if(sparkConfig.isLocal()) {
			conf.setMaster("local");
		}
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
		SQLContext sqlContext = sparkConfig.isLocal() ? new SQLContext(sc.sc()) : new HiveContext(sc.sc());
		if(sparkConfig.isLocal()) {
			MockData.mock(sc, sqlContext);
		}
		long taskid = sparkConfig.isLocal() ? sparkConfig.getTaskSession() : Long.parseLong(args[0]);
		Task task = taskRepository.findById(taskid).orElse(null);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		}
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, task.toParam());
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
//		sessionid2actionRDD.checkpoint();
		// 数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sqlContext, sessionid2actionRDD);
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, task.toParam(), sessionAggrStatAccumulator);
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
		randomExtractSession(sc, task.getTaskId(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskId(), sessionid2detailRDD);
		getTop10Session(sc, task.getTaskId(), top10CategoryList, sessionid2detailRDD);
		sc.close();
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
//		return actionDF.javaRDD().repartition(1000);
		return sqlContext.sql(sql).javaRDD();
	}

	/**
	 * 生成模拟数据（只有本地模式，才会去生成模拟数据）
	 * @param sc
	 * @param sqlContext
	 */
	private void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		if(sparkConfig.isLocal()) {
			MockData.mock(sc, sqlContext);
		}
	}

	/**
	 * 获取sessionid2到访问行为数据的映射的RDD
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapPartitionsToPair(iterator ->
				StreamSupport
						.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
						.map(row -> new Tuple2<>(row.getString(2), row))
						.collect(Collectors.toList())
		);
	}

	/**
	 * 对行为数据按session粒度进行聚合
	 */
	private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext,
			JavaPairRDD<String, Row> sessinoid2actionRDD) {
		// 对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey();
		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		// 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD
				.mapToPair(tuple -> {
					String sessionid = tuple._1;
					Iterator<Row> iterator = tuple._2.iterator();
					StringBuilder searchKeywordsBuffer = new StringBuilder();
					StringBuilder clickCategoryIdsBuffer = new StringBuilder();
					Long userid = null;
					// session的起始和结束时间
					Date startTime = null;
					Date endTime = null;
					// session的访问步长
					int stepLength = 0;
					// 遍历session所有的访问行为
					while(iterator.hasNext()) {
						// 提取每个访问行为的搜索词字段和点击品类字段
						Row row = iterator.next();
						if(userid == null) {
							userid = row.getLong(1);
						}
						String searchKeyword = row.getString(5);
						Long clickCategoryId = row.getLong(6);
						// 实际上这里要对数据说明一下
						// 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
						// 其实，只有搜索行为，是有searchKeyword字段的
						// 只有点击品类的行为，是有clickCategoryId字段的
						// 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
						// 我们决定是否将搜索词或点击品类id拼接到字符串中去
						// 首先要满足：不能是null值
						// 其次，之前的字符串中还没有搜索词或者点击品类id
						if(StringUtils.isNotEmpty(searchKeyword)) {
							if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
								searchKeywordsBuffer.append(searchKeyword).append(",");
							}
						}
						if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
							clickCategoryIdsBuffer.append(clickCategoryId).append(",");
						}
						// 计算session开始和结束时间
						Date actionTime = DateUtils.parseTime(row.getString(4));
						if(startTime == null) {
							startTime = actionTime;
						}
						if(endTime == null) {
							endTime = actionTime;
						}
						if(actionTime.before(startTime)) {
							startTime = actionTime;
						}
						if(actionTime.after(endTime)) {
							endTime = actionTime;
						}
						// 计算session访问步长
						stepLength++;
					}
			String searchKeywords = Arrays.stream(searchKeywordsBuffer.toString().split(","))
					.filter(StringUtils::isNotBlank).collect(Collectors.joining(","));
					String clickCategoryIds = Arrays.stream(clickCategoryIdsBuffer.toString().split(","))
					.filter(StringUtils::isNotBlank).collect(Collectors.joining(","));
					// 计算session访问时长（秒）
					long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
			String partAggrInfo = ImmutableMap.builder()
					.put(Constants.FIELD_SESSION_ID, sessionid)
					.put(Constants.FIELD_SEARCH_KEYWORDS, searchKeywords)
					.put(Constants.FIELD_CLICK_CATEGORY_IDS, clickCategoryIds)
					.put(Constants.FIELD_VISIT_LENGTH, visitLength)
					.put(Constants.FIELD_STEP_LENGTH, stepLength)
					.put(Constants.FIELD_START_TIME, DateUtils.formatTime(startTime))
					.build()
					.entrySet()
					.stream()
					.map(e -> e.getKey() + "=" + e.getValue())
					.collect(Collectors.joining("|"));
					return new Tuple2<>(userid, partAggrInfo);
				});
		// 查询所有用户数据，并映射成<userid,Row>的格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(row -> new Tuple2<>(row.getLong(0), row));
		/*
		 * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
		 * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
		 * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
		 */
		// 将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
		// 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		return userid2FullInfoRDD
				.mapToPair(tuple -> {
					String partAggrInfo = tuple._2._1;
					Row userInfoRow = tuple._2._2;
					String sessionid = Arrays.stream(partAggrInfo.split("\\|"))
							.filter(kv -> kv.contains(Constants.FIELD_SESSION_ID) && Constants.FIELD_SESSION_ID.equals(kv.split("=")[0]))
							.map(kv -> kv.split("=")[1])
							.findFirst().orElseGet(() -> partAggrInfo);
					int age = userInfoRow.getInt(3);
					String professional = userInfoRow.getString(4);
					String city = userInfoRow.getString(5);
					String sex = userInfoRow.getString(6);
					String fullAggrInfo = ImmutableMap.builder()
							.put(Constants.FIELD_AGE, age)
							.put(Constants.FIELD_PROFESSIONAL, professional)
							.put(Constants.FIELD_CITY, city)
							.put(Constants.FIELD_SEX, sex)
							.build()
							.entrySet()
							.stream()
							.map(e -> e.getKey() + "=" + e.getValue())
							.collect(Collectors.joining("|", partAggrInfo + "|", ""));
					return new Tuple2<>(sessionid, fullAggrInfo);
				});
	}

	/**
	 * 过滤session数据，并进行聚合统计
	 * @param sessionid2AggrInfoRDD
	 * @param param
	 * @return
	 */
	private JavaPairRDD<String, String> filterSessionAndAggrStat(
	        JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			final Param param,
            final Accumulator<String> sessionAggrStatAccumulator) {
		// 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
		// 此外，这里其实大家不要觉得是多此一举
		// 其实我们是给后面的性能优化埋下了一个伏笔
		String startAge = param.getStartAge();
		String endAge = param.getEndAge();
		String professionals = param.getProfessionals();
		String cities = param.getCities();
		String sex = param.getSex();
		String keywords = param.getKeywords();
		String categoryIds = param.getCategoryIds();
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		if(_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		final String parameter = _parameter;
		// 根据筛选参数进行过滤
		return sessionid2AggrInfoRDD.filter(tuple -> {
			// 首先，从tuple中，获取聚合数据
			String aggrInfo = tuple._2;
			// 接着，依次按照筛选条件进行过滤
			// 按照年龄范围进行过滤（startAge、endAge）
			if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
				return false;
			}
			// 按照职业范围进行过滤（professionals）
			// 互联网,IT,软件
			// 互联网
			if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
				return false;
			}
			// 按照城市范围进行过滤（cities）
			// 北京,上海,广州,深圳
			// 成都
			if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
				return false;
			}
			// 按照性别进行过滤
			// 男/女
			// 男，女
			if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
				return false;
			}
			// 按照搜索词进行过滤
			// 我们的session可能搜索了 火锅,蛋糕,烧烤
			// 我们的筛选条件可能是 火锅,串串香,iphone手机
			// 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
			// 任何一个搜索词相当，即通过
			if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
				return false;
			}
			// 按照点击品类id进行过滤
			if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
				return false;
			}
			// 如果经过了之前的多个过滤条件之后，程序能够走到这里
			// 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
			// 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
			// 进行相应的累加计数
			// 主要走到这一步，那么就是需要计数的session
			sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
			// 计算出session的访问时长和访问步长的范围，并进行相应的累加
			Map<String, Long> map = Arrays.stream(aggrInfo.split("\\|"))
					.map(kv -> kv.split("="))
					.collect(Collectors.toMap(arr -> arr[0], arr -> Long.parseLong(arr[1])));
			long visitLength = map.get(Constants.FIELD_VISIT_LENGTH);
			long stepLength = map.get(Constants.FIELD_STEP_LENGTH);
			calculateVisitLength(visitLength, sessionAggrStatAccumulator);
			calculateStepLength(stepLength, sessionAggrStatAccumulator);
			return true;
		});
	}


	/**
	 * 计算访问时长范围
	 * @param visitLength
	 * @param sessionAggrStatAccumulator
	 */
	private void calculateVisitLength(long visitLength, Accumulator<String> sessionAggrStatAccumulator) {
        ImmutableMap.<Range<Integer>, String>builder()
                .put(Range.closed(1, 3), Constants._1s_3s)
                .put(Range.closed(4, 6), Constants._4s_6s)
                .put(Range.closed(7, 9), Constants._7s_9s)
                .put(Range.closed(10, 30), Constants._10s_30s)
                .put(Range.openClosed(30, 60), Constants._30s_60s)
                .put(Range.openClosed(60, 180), Constants._1m_3m)
                .put(Range.openClosed(180, 600), Constants._3m_10m)
                .put(Range.openClosed(600, 1800), Constants._10m_30m)
                .put(Range.greaterThan(1800), Constants._30m)
                .build()
                .entrySet().stream()
                .filter(e -> e.getKey().contains((int) visitLength))
                .findFirst()
                .map(Map.Entry::getValue)
                .ifPresent(sessionAggrStatAccumulator::add);
	}

	/**
	 * 计算访问步长范围
	 * @param stepLength
	 * @param sessionAggrStatAccumulator
	 */
	private void calculateStepLength(long stepLength, Accumulator<String> sessionAggrStatAccumulator) {
        ImmutableMap.<Range<Integer>, String>builder()
                .put(Range.closed(1, 3), Constants._1_3)
                .put(Range.closed(4, 6), Constants._4_6)
                .put(Range.closed(7, 9), Constants._7_9)
                .put(Range.closed(10, 30), Constants._10_30)
                .put(Range.openClosed(30, 60), Constants._30_60)
                .put(Range.greaterThan(60), Constants._60)
                .build()
                .entrySet().stream()
                .filter(e -> e.getKey().contains((int) stepLength))
                .findFirst()
                .map(Map.Entry::getValue)
                .ifPresent(sessionAggrStatAccumulator::add);
	}

	/**
	 * 获取通过筛选条件的session的访问明细数据RDD
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2detailRDD(JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		return sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2));
	}

	/**
	 * 随机抽取session
	 * @param sessionid2AggrInfoRDD
	 */
	private void randomExtractSession(JavaSparkContext sc,
									  final long taskId,
									  JavaPairRDD<String, String> sessionid2AggrInfoRDD,
									  JavaPairRDD<String, Row> sessionid2actionRDD) {
		// 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD
				.mapToPair(tuple -> {
					String startTime = Arrays.stream(tuple._2.split("\\|"))
							.filter(kv -> kv.startsWith(Constants.FIELD_START_TIME) && Constants.FIELD_START_TIME.equals(kv.split("=")[0]))
							.map(kv -> kv.split("=")[1])
							.findFirst().orElse("");
					String dateHour = DateUtils.getDateHour(startTime);
					return new Tuple2<>(dateHour, tuple._2);
				});
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		//第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引,将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = countMap
				.entrySet()
				.stream()
                .collect(Collectors.toMap(e -> e.getKey().split("_")[0],
                        e -> ImmutableMap.of(e.getKey().split("_")[1], Long.parseLong(String.valueOf(e.getValue())))));
		// 开始实现我们的按时间比例随机抽取算法
		// 总共要抽取100个session，先按照天数，进行平分
		int extractNumberPerDay = 100 / dateHourCountMap.size();
		// <date,<hour,(3,5,20,102)>>
		/*
		 * session随机抽取功能
		 * 用到了一个比较大的变量，随机抽取索引map
		 * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
		 * 还是比较消耗内存和网络传输性能的
		 * 将map做成广播变量
		 */
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();
		Random random = new Random();
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			// 计算出这一天的session总数
			long sessionCount = hourCountMap.values().stream().mapToLong(Long::longValue).sum();
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.computeIfAbsent(date, k -> new HashMap<>());
			// 遍历每个小时
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				// 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
				// 就可以计算出，当前小时需要抽取的session数量
				int hourExtractNumber = (int)(((double)count / (double)sessionCount) * extractNumberPerDay);
				if(hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}
				// 先获取当前小时的存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.computeIfAbsent(hour, k -> new ArrayList<>());
				// 生成上面计算出来的数量的随机数
				for(int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);
					while(extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}

		/*
		 * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
		 */
		Function<Map.Entry<String, Map<String, List<Integer>>>, Map<String, IntList>> valueMapper = entry ->
				entry.getValue().entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, e -> new IntArrayList(e.getValue())));
		Map<String, Map<String, IntList>> fastUtilDateHourExtractMap = dateHourExtractMap.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, valueMapper));
		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap);
		// 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
		time2sessionidRDD
				.groupByKey()
				.flatMapToPair(tuple -> {
					String dateHour = tuple._1;
					String date = dateHour.split("_")[0];
					String hour = dateHour.split("_")[1];
					Iterator<String> iterator = tuple._2.iterator();
					Map<String, Map<String, IntList>> dateHourExtractMap1 = dateHourExtractMapBroadcast.value();
					List<Integer> extractIndexList = dateHourExtractMap1.get(date).get(hour);
					List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
					int index = 0;
					while(iterator.hasNext()) {
						String sessionAggrInfo = iterator.next();
						if(extractIndexList.contains(index)) {
							// 将数据写入MySQL
							SessionRandomExtract extract = SessionRandomExtract.ctor(taskId, sessionAggrInfo);
							sessionRandomExtractRepository.save(extract);
							extractSessionIds.add(new Tuple2<>(extract.getSessionId(), extract.getSessionId()));
						}
						index++;
					}
					return extractSessionIds;
				})
				.join(sessionid2actionRDD)
				.foreachPartition(iterator -> {
					List<SessionDetail> details = StreamSupport
							.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
							.map(Tuple2::_2)
							.map(Tuple2::_2)
							.map(row -> SessionDetail.ctor(taskId, row))
							.collect(Collectors.toList());
					sessionDetailRepository.saveAll(details);
				});
	}

	/**
	 * 计算各session范围占比，并写入MySQL
	 * @param value
	 */
	private void calculateAndPersistAggrStat(String value, long taskId) {
        Map<String, Long> map = Arrays.stream(value.split("\\|"))
                .collect(Collectors.toMap(kv -> kv.split("=")[0], kv -> Long.parseLong(kv.split("=")[1])));
        long sessionCount = map.remove(Constants.SESSION_COUNT);
        Map<String, Double> result = map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new BigDecimal(e.getValue())
                        .divide(BigDecimal.valueOf(sessionCount), 2, RoundingMode.HALF_UP).doubleValue()));
        // 从Accumulator统计串中获取值
		// 将统计结果封装为Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskId(taskId);
		sessionAggrStat.setSessionCount(sessionCount);
		sessionAggrStat.setResult(new Gson().toJson(result));
		sessionAggrStatRepository.save(sessionAggrStat);
	}

	/**
	 * 获取top10热门品类
	 */
	private List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> sessionid2detailRDD) {
		//第一步：获取符合条件的session访问过的所有品类
		// 获取session访问过的所有品类id
		// 访问过：指的是，点击过、下单过、支付过的品类
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD
				.flatMapToPair(tuple -> {
					Row row = tuple._2;
					List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
					Long clickCategoryId = row.getLong(6);
					list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
					String orderCategoryIds = row.getString(8);
					if(orderCategoryIds != null) {
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						for(String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
						}
					}
					String payCategoryIds = row.getString(10);
					if(payCategoryIds != null) {
						String[] payCategoryIdsSplited = payCategoryIds.split(",");
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
						}
					}
					return list;
				});

		/*
		 * 必须要进行去重
		 * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
		 * 最后很可能会拿到重复的数据
		 */
		categoryidRDD = categoryidRDD.distinct();
		/*
		 * 第二步：计算各品类的点击、下单和支付的次数
		 */
		// 访问明细中，其中三种访问行为是：点击、下单和支付
		// 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
		// 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
		// 计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

		/*
		 * 第三步：join各品类与它的点击、下单和支付的次数
		 *
		 * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
		 *
		 * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
		 * 比如，有的品类，就只是被点击过，但是没有人下单和支付
		 *
		 * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
		 * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
		 * 只不过，没有join到的那个数据，就是0了
		 */
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

		/*
		 * 第四步：自定义二次排序key
		 */
		/*
		 * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
		 */
		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD
				.mapToPair(tuple -> {
					String countInfo = tuple._2;
					Map<String, Long> map = Arrays.stream(tuple._2.split("\\|"))
							.map(kv -> kv.split("="))
							.collect(Collectors.toMap(arr -> arr[0], arr -> Long.parseLong(arr[1])));
					long clickCount = map.getOrDefault(Constants.FIELD_CLICK_COUNT, 0L);
					long orderCount = map.getOrDefault(Constants.FIELD_ORDER_COUNT, 0L);
					long payCount = map.getOrDefault(Constants.FIELD_PAY_COUNT, 0L);
					CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
					return new Tuple2<>(sortKey, countInfo);
				});
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD
				.sortByKey(false);
		/*
		 * 第六步：用take(10)取出top10热门品类，并写入MySQL
		 */
		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
		for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
			Map<String, Long> map = Arrays.stream(tuple._2.split("\\|"))
					.map(kv -> kv.split("="))
					.collect(Collectors.toMap(arr -> arr[0], arr -> Long.parseLong(arr[1])));
			long categoryid = map.getOrDefault(Constants.FIELD_CATEGORY_ID, 0L);
			long clickCount = map.getOrDefault(Constants.FIELD_CLICK_COUNT, 0L);
			long orderCount = map.getOrDefault(Constants.FIELD_ORDER_COUNT, 0L);
			long payCount = map.getOrDefault(Constants.FIELD_PAY_COUNT, 0L);
			TopCategory category = new TopCategory();
			category.setTaskId(taskid);
			category.setCategoryId(categoryid);
			category.setClickCount(clickCount);
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);
			topCategoryRepository.save(category);
		}
		return top10CategoryList;
	}

	/**
	 * 获取各品类点击次数RDD
	 * @param sessionId2detailRDD
	 * @return
	 */
	private JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
		/*
		 * 说明一下：
		 * 这儿，是对完整的数据进行了filter过滤，过滤出来点击行为的数据
		 * 点击行为的数据其实只占总数据的一小部分
		 * 所以过滤以后的RDD，每个partition的数据量，很有可能跟我们之前说的一样，会很不均匀
		 * 而且数据量肯定会变少很多
		 * 所以针对这种情况，还是比较合适用一下coalesce算子的，在filter过后去减少partition的数量
		 */
		return sessionId2detailRDD
				.filter(tuple -> tuple._2.get(6) != null)
				// .coalesce(100)
				/*
				 * 对这个coalesce操作做一个说明
				 * 我们在这里用的模式都是local模式，主要是用来测试，所以local模式下，不用去设置分区和并行度的数量
				 * local模式自己本身就是进程内模拟的集群来执行，本身性能就很高
				 * 而且对并行度、partition数量都有一定的内部的优化
				 * 这里我们再自己去设置，就有点画蛇添足
				 * 但是就是跟大家说明一下，coalesce算子的使用，即可
				 */
				.mapToPair(tuple -> new Tuple2<>(tuple._2.getLong(6), 1L))
				.reduceByKey(Long::sum);
	}

	/**
	 * 获取各品类的下单次数RDD
	 * @param sessionId2detailRDD
	 * @return
	 */
	private JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
		return sessionId2detailRDD
				.filter(tuple -> tuple._2.getString(8) != null)
				.flatMapToPair(this::getTuple2LongLongPairFlatMapFunction)
				.reduceByKey(Long::sum);
	}

	private List<Tuple2<Long, Long>> getTuple2LongLongPairFlatMapFunction(Tuple2<String, Row> tuple) {
		 return Arrays.stream(tuple._2.getString(8).split(","))
				.map(str -> new Tuple2<>(Long.valueOf(str), 1L)).collect(Collectors.toList());
	}

	/**
	 * 获取各个品类的支付次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		return sessionid2detailRDD
				.filter(tuple -> tuple._2.getString(10) != null)
				.flatMapToPair(this::getTuple2LongLongPairFlatMapFunction)
				.reduceByKey(Long::sum);
	}

	/**
	 * 连接品类RDD与数据RDD
	 * @param categoryIdRDD
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryIdRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		// 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
		// 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
		return categoryIdRDD
				.leftOuterJoin(clickCategoryId2CountRDD)
				.mapToPair(tuple -> new Tuple2<>(tuple._1, Constants.FIELD_CATEGORY_ID + "=" + tuple._1 + "|" + Constants.FIELD_CLICK_COUNT + "=" + tuple._2._2.or(0L)))
				.leftOuterJoin(orderCategoryId2CountRDD)
				.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + tuple._2._2.or(0L)))
				.leftOuterJoin(payCategoryId2CountRDD)
				.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._1 + "|" + Constants.FIELD_PAY_COUNT + "=" + tuple._2._2.or(0L)));
	}

	/**
	 * 获取top10活跃session
	 * @param taskid
	 * @param sessionid2detailRDD
	 */
	private void getTop10Session(JavaSparkContext sc,
								 final long taskid,
								 List<Tuple2<CategorySortKey, String>> top10CategoryList,
								 JavaPairRDD<String, Row> sessionid2detailRDD) {
		// 第一步：将top10热门品类的id，生成一份RDD

		List<Tuple2<Long, Long>> top10CategoryIdList = top10CategoryList.stream()
				.flatMap(category -> Arrays.stream(category._2.split("\\|"))
						.filter(kv -> kv.startsWith(Constants.FIELD_CATEGORY_ID) && Constants.FIELD_CATEGORY_ID.equals(kv.split("=")[0]))
						.map(kv -> kv.split("=")[1])
						.filter(StringUtils::isNotBlank)
				)
				.map(Long::parseLong)
				.map(categoryId -> new Tuple2<>(categoryId, categoryId))
				.collect(Collectors.toList());
		JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
		// 第二步：计算top10品类被各session点击的次数
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailRDD
				.groupByKey()
				// 返回结果，<categoryid,sessionid,count>格式
				.flatMapToPair(tuple -> StreamSupport
						.stream(tuple._2.spliterator(), false)
						.map(row -> row.getLong(6))
						.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
						.entrySet().stream()
						.map(entry -> new Tuple2<>(entry.getKey(), tuple._1 + "," + entry.getValue()))
						.collect(Collectors.toList())
				);
		// 获取到to10热门品类，被各个session点击的次数
		top10CategoryIdRDD
				.join(categoryid2sessionCountRDD)
				.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2))
				// 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
				.groupByKey()
				.flatMapToPair(tuple -> {
					Iterator<String> iterator = tuple._2.iterator();
					// 定义取topn的排序数组
					String[] top10Sessions = new String[10];
					while (iterator.hasNext()) {
						String sessionCount = iterator.next();
						long count = Long.parseLong(sessionCount.split(",")[1]);
						// 遍历排序数组
						for (int i = 0; i < top10Sessions.length; i++) {
							// 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
							if (top10Sessions[i] == null) {
								top10Sessions[i] = sessionCount;
								break;
							} else {
								long _count = Long.parseLong(top10Sessions[i].split(",")[1]);
								// 如果sessionCount比i位的sessionCount要大
								if (count > _count) {
									// 从排序数组最后一位开始，到i位，所有数据往后挪一位
									System.arraycopy(top10Sessions, i, top10Sessions, i + 1, 9 - i);
									// 将i位赋值为sessionCount
									top10Sessions[i] = sessionCount;
									break;
								}
								// 比较小，继续外层for循环
							}
						}
					}
					// 将数据写入MySQL表
					List<TopSession> sessions = Arrays.stream(top10Sessions)
							.filter(Objects::nonNull)
							.map(sessionCount -> {
								TopSession session = new TopSession();
								session.setTaskId(taskid);
								session.setCategoryId(tuple._1);
								session.setSessionId(sessionCount.split(",")[0]);
								session.setClickCount(Long.parseLong(sessionCount.split(",")[1]));
								return session;
							}).collect(Collectors.toList());
					topSessionRepository.saveAll(sessions);
					return sessions.stream()
							.map(TopSession::getSessionId)
							.map(sessionId -> new Tuple2<>(sessionId, sessionId))
							.collect(Collectors.toList());
				})
				// 第四步：获取top10活跃session的明细数据，并写入MySQL
				.join(sessionid2detailRDD)
				.foreach(tuple -> sessionDetailRepository.save(SessionDetail.ctor(taskid, tuple._2._2)));
	}

}
