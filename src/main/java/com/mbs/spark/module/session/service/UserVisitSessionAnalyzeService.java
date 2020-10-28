package com.mbs.spark.module.session.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.gson.Gson;
import com.mbs.spark.conf.SparkConfigurer;
import com.mbs.spark.constant.Constants;
import com.mbs.spark.module.product.model.Top10Category;
import com.mbs.spark.module.session.CategorySortKey;
import com.mbs.spark.module.session.SessionAggrStatAccumulator;
import com.mbs.spark.module.session.model.SessionAggrStat;
import com.mbs.spark.module.session.model.SessionDetail;
import com.mbs.spark.module.session.model.SessionRandomExtract;
import com.mbs.spark.module.session.model.Top10Session;
import com.mbs.spark.module.session.repository.SessionAggrStatRepository;
import com.mbs.spark.module.session.repository.SessionDetailRepository;
import com.mbs.spark.module.session.repository.SessionRandomExtractRepository;
import com.mbs.spark.module.session.repository.Top10CategoryRepository;
import com.mbs.spark.module.session.repository.Top10SessionRepository;
import com.mbs.spark.module.task.model.Param;
import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.repository.TaskRepository;
import com.mbs.spark.test.MockData;
import com.mbs.spark.tools.DateUtils;
import com.mbs.spark.tools.StringUtils;
import com.mbs.spark.tools.ValidUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class UserVisitSessionAnalyzeService {

	@Autowired
	SessionAggrStatRepository sessionAggrStatRepository;
	@Autowired
	SessionDetailRepository sessionDetailRepository;
	@Autowired
	SessionRandomExtractRepository sessionRandomExtractRepository;
	@Autowired
	TaskRepository taskRepository;
	@Autowired
	Top10CategoryRepository top10CategoryRepository;
	@Autowired
	Top10SessionRepository top10SessionRepository;
	@Autowired
	SparkConfigurer sparkConfigurer;

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
		if(sparkConfigurer.isLocal()) {
			conf.setMaster("local");
		}
		/*
		 * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
		 * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
		 * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
		 * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
		 */

		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
		SQLContext sqlContext = sparkConfigurer.isLocal() ? new SQLContext(sc.sc()) : new HiveContext(sc.sc());

		// 生成模拟测试数据
		if(sparkConfigurer.isLocal()) {
			MockData.mock(sc, sqlContext);
		}

		// 创建需要使用的DAO组件
		// 首先得查询出来指定的任务，并获取任务的查询参数
		long taskid = sparkConfigurer.isLocal() ? sparkConfigurer.getTaskSession() : Long.parseLong(args[0]);
		Task task = taskRepository.findById(taskid).orElse(null);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		}

		// 如果要进行session粒度的数据聚合
		// 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据

		/*
		 * actionRDD，就是一个公共RDD
		 * 第一，要用ationRDD，获取到一个公共的sessionid为key的PairRDD
		 * 第二，actionRDD，用在了session聚合环节里面
		 *
		 * sessionid为key的PairRDD，是确定了，在后面要多次使用的
		 * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
		 * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
		 *
		 * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
		 *
		 */
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, task.toParam());
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);

		/*
		 * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
		 *
		 * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
		 * StorageLevel.MEMORY_ONLY_SER()，第二选择
		 * StorageLevel.MEMORY_AND_DISK()，第三选择
		 * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
		 * StorageLevel.DISK_ONLY()，第五选择
		 *
		 * 如果内存充足，要使用双副本高可靠机制
		 * 选择后缀带_2的策略
		 * StorageLevel.MEMORY_ONLY_2()
		 *
		 */
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
//		sessionid2actionRDD.checkpoint();

		// 首先，可以将行为数据，按照session_id进行groupByKey分组
		// 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		// 与用户信息数据，进行join
		// 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
		// 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sqlContext, sessionid2actionRDD);

		// 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		// 相当于我们自己编写的算子，是要访问外面的任务参数对象的
		// 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的

		// 重构，同时进行过滤和统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, task.toParam(), sessionAggrStatAccumulator);
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

		// 生成公共的RDD：通过筛选条件的session的访问明细数据

		/*
		 * 重构：sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
		 */
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

		/*
		 * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
		 *
		 * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
		 * 再进行。。。
		 *
		 * 如果没有action的话，那么整个程序根本不会运行。。。
		 *
		 * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
		 * 不对！！！
		 *
		 * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
		 *
		 * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
		 */
		randomExtractSession(sc, task.getTaskId(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

		/*
		 * 特别说明
		 * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
		 * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
		 * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
		 * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
		 */

		// 计算出各个范围的session占比，并写入MySQL
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());

		/*
		 * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
		 *
		 * 如果不进行重构，直接来实现，思路：
		 * 1、actionRDD，映射成<sessionid,Row>的格式
		 * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
		 * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
		 * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
		 * 5、将最后计算出来的结果，写入MySQL对应的表中
		 *
		 * 普通实现思路的问题：
		 * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
		 * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
		 * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
		 *
		 * 重构实现思路：
		 * 1、不要去生成任何新的RDD（处理上亿的数据）
		 * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
		 * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
		 * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
		 * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
		 * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
		 * 		半个小时，或者数个小时
		 *
		 * 开发Spark大型复杂项目的一些经验准则：
		 * 1、尽量少生成RDD
		 * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
		 * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
		 * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
		 * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
		 * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
		 * 4、无论做什么功能，性能第一
		 * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
		 * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
		 *
		 * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
		 * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
		 * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
		 * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
		 * 		此时，对于用户体验，简直就是一场灾难
		 *
		 * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
		 *
		 * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
		 * 		如果采用第二种方案，那么其实就是性能优先
		 *
		 * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
		 * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
		 * 		积累了，处理各种问题的经验
		 *
		 */
		// 获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskId(), sessionid2detailRDD);
		// 获取top10活跃session
		getTop10Session(sc, task.getTaskId(), top10CategoryList, sessionid2detailRDD);
		// 关闭Spark上下文
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
		/*
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 *
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
//		return actionDF.javaRDD().repartition(1000);
		return sqlContext.sql(sql).javaRDD();
	}

	/**
	 * 生成模拟数据（只有本地模式，才会去生成模拟数据）
	 * @param sc
	 * @param sqlContext
	 */
	private void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		if(sparkConfigurer.isLocal()) {
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
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(tuple -> {
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
					String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
					String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
					// 计算session访问时长（秒）
					long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
					// 大家思考一下
					// 我们返回的数据格式，即使<sessionid,partAggrInfo>
					// 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
					// 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
					// 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
					// 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
					// 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
					// 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
					// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
					// 然后再直接将返回的Tuple的key设置成sessionid
					// 最后的数据格式，还是<sessionid,fullAggrInfo>
					// 聚合数据，用什么样的格式进行拼接？
					// 我们这里统一定义，使用key=value|key=value
					String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
							+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
							+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
							+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
							+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
							+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
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
		return userid2FullInfoRDD.mapToPair(tuple -> {
					String partAggrInfo = tuple._2._1;
					Row userInfoRow = tuple._2._2;
					String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
					int age = userInfoRow.getInt(3);
					String professional = userInfoRow.getString(4);
					String city = userInfoRow.getString(5);
					String sex = userInfoRow.getString(6);
					String fullAggrInfo = partAggrInfo + "|"
							+ Constants.FIELD_AGE + "=" + age + "|"
							+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
							+ Constants.FIELD_CITY + "=" + city + "|"
							+ Constants.FIELD_SEX + "=" + sex;
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
			long visitLength = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)));
			long stepLength = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH)));
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
		//第一步，计算出每天每小时的session数量
		// 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD
				.mapToPair(tuple -> {
					String startTime = StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_START_TIME);
					String dateHour = DateUtils.getDateHour(startTime);
					return new Tuple2<>(dateHour, tuple._2);
				});
		/*
		 * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
		 * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
		 * 首先抽取出的session的聚合数据，写入session_random_extract表
		 * 所以第一个RDD的value，应该是session聚合数据
		 */
		// 得到每天每小时的session数量
		/*
		 * 每天每小时的session数量的计算
		 * 是有可能出现数据倾斜的吧，这个是没有疑问的
		 * 比如说大部分小时，一般访问量也就10万；但是，中午12点的时候，高峰期，一个小时1000万
		 * 这个时候，就会发生数据倾斜
		 * 我们就用这个countByKey操作，给大家演示第三种和第四种方案
		 */
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		//第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
		// 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = countMap.entrySet().stream()
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
		/*
		 * 广播变量，很简单
		 * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
		 */
		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap);
		/*
		 * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
		 */
		// 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
		// 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
		// 然后呢，会遍历每天每小时的session
		// 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
		// 那么抽取该session，直接写入MySQL的random_extract_session表
		// 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
		// 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD
				.flatMapToPair(tuple -> {
					String dateHour = tuple._1;
					String date = dateHour.split("_")[0];
					String hour = dateHour.split("_")[1];
					Iterator<String> iterator = tuple._2.iterator();
					/*
					 * 使用广播变量的时候
					 * 直接调用广播变量（Broadcast类型）的value() / getValue()
					 * 可以获取到之前封装的广播变量
					 */
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
				});

		/*
		 * 第四步：获取抽取出来的session的明细数据
		 */
		extractSessionidsRDD
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
					long clickCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT)));
					long orderCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT)));
					long payCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT)));
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
			String countInfo = tuple._2;
			long categoryid = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID)));
			long clickCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT)));
			long orderCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT)));
			long payCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT)));
			Top10Category category = new Top10Category();
			category.setTaskId(taskid);
			category.setCategoryId(categoryid);
			category.setClickCount(clickCount);
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);
			top10CategoryRepository.save(category);
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
				.map(category -> StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID))
				.filter(Objects::nonNull)
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
					List<Top10Session> sessions = Arrays.stream(top10Sessions)
							.filter(Objects::nonNull)
							.map(sessionCount -> {
								Top10Session session = new Top10Session();
								session.setTaskId(taskid);
								session.setCategoryId(tuple._1);
								session.setSessionId(sessionCount.split(",")[0]);
								session.setClickCount(Long.parseLong(sessionCount.split(",")[1]));
								return session;
							}).collect(Collectors.toList());
					top10SessionRepository.saveAll(sessions);
					return sessions.stream()
							.map(Top10Session::getSessionId)
							.map(sessionId -> new Tuple2<>(sessionId, sessionId))
							.collect(Collectors.toList());
				})
				// 第四步：获取top10活跃session的明细数据，并写入MySQL
				.join(sessionid2detailRDD)
				.foreach(tuple -> sessionDetailRepository.save(SessionDetail.ctor(taskid, tuple._2._2)));
	}

}
