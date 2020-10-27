package com.mbs.spark.module.session.model;

import com.mbs.spark.constant.Constants;
import com.mbs.spark.tools.StringUtils;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

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
	@Column(name = "id")
	private Long id;
	private long taskId;
	private String sessionId;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;

	public static SessionRandomExtract ctor(long taskId, String sessionAggrInfo) {
		SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
		sessionRandomExtract.setTaskId(taskId);
		sessionRandomExtract.setSessionId(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID));
		sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
		sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
		sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
		return sessionRandomExtract;
	}
}
