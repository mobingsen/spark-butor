package com.mbs.spark.module.session.model;

import com.mbs.spark.constant.Constants;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 随机抽取的session
 */
@Getter
@Setter
@Entity
public class SessionRandomExtract {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private long taskId;
	private String sessionId;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;

	public static SessionRandomExtract ctor(long taskId, String sessionAggrInfo) {
		Map<String, String> map = Arrays.stream(sessionAggrInfo.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
		SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
		sessionRandomExtract.setTaskId(taskId);
		sessionRandomExtract.setSessionId(map.getOrDefault(Constants.FIELD_SESSION_ID, ""));
		sessionRandomExtract.setStartTime(map.getOrDefault(Constants.FIELD_START_TIME, ""));
		sessionRandomExtract.setSearchKeywords(map.getOrDefault(Constants.FIELD_SEARCH_KEYWORDS, ""));
		sessionRandomExtract.setClickCategoryIds(map.getOrDefault(Constants.FIELD_CLICK_CATEGORY_IDS, ""));
		return sessionRandomExtract;
	}
}
