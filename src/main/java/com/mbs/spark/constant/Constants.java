package com.mbs.spark.constant;

/**
 * 常量接口
 */
public interface Constants {

	/**
	 * Spark作业相关的常量
	 */
	String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
	String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateSpark";
	String FIELD_SESSION_ID = "sessionid";
	String FIELD_SEARCH_KEYWORDS = "searchKeywords";
	String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
	String FIELD_AGE = "age";
	String FIELD_PROFESSIONAL = "professional";
	String FIELD_CITY = "city";
	String FIELD_SEX = "sex";
	String FIELD_VISIT_LENGTH = "visitLength";
	String FIELD_STEP_LENGTH = "stepLength";
	String FIELD_START_TIME = "startTime";
	String FIELD_CLICK_COUNT = "clickCount";
	String FIELD_ORDER_COUNT = "orderCount";
	String FIELD_PAY_COUNT = "payCount";
	String FIELD_CATEGORY_ID = "categoryid";

	String SESSION_COUNT = "session_count";

	String _1s_3s = "1s_3s";
	String _4s_6s = "4s_6s";
	String _7s_9s = "7s_9s";
	String _10s_30s = "10s_30s";
	String _30s_60s = "30s_60s";
	String _1m_3m = "1m_3m";
	String _3m_10m = "3m_10m";
	String _10m_30m = "10m_30m";
	String _30m = "30m";

	String _1_3 = "1_3";
	String _4_6 = "4_6";
	String _7_9 = "7_9";
	String _10_30 = "10_30";
	String _30_60 = "30_60";
	String _60 = "60";

	/**
	 * 任务相关的常量
	 */
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";
}
