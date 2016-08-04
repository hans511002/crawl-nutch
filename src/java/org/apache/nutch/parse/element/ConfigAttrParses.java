package org.apache.nutch.parse.element;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.colrule.ColSegmentParse;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.urlfilter.UrlPathMatch;

/**
 * 关联配置地址相关的属性ID a.ROOT_SITE_ID,a.MEDIA_TYPE_ID,a.MEDIA_LEVEL_ID,a.TOPIC_TYPE_ID,a.area_id
 * 
 * @author hans
 * 
 */
public class ConfigAttrParses extends SegMentParse {
	private static final long serialVersionUID = -1501883972712797986L;
	Configuration conf = null;
	public static UrlPathMatch baseUrlAttr = null;

	public static class TopicTypeRule implements java.io.Serializable {
		private static final long serialVersionUID = 2029233115724431244L;
		public Long topicTypeId;
		public ColSegmentParse parse;
		public List<TopicTypeRule> subRule;

		public boolean parse(String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util) {
			try {
				if (parse != null && parse.parseSegMent(unreverseKey, page, psg, util, null)) {
					psg.setTopicTypeId(topicTypeId);
					return true;
				}
			} catch (Exception e) {
			}
			if (subRule == null)
				return false;
			for (TopicTypeRule tpRule : subRule) {
				try {
					if (tpRule.parse(unreverseKey, page, psg, util))
						return true;
				} catch (Exception e) {
				}
			}
			return false;
		}
	}

	// 栏目ID,
	public static HashMap<Long, TopicTypeRule> topicTypeRules = null;
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	public ConfigAttrParses() {
	}

	@Override
	public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg) throws IOException {
		if (baseUrlAttr == null)
			NutchConstant.getSegmentParseRules(conf);
		if (baseUrlAttr == null) {
			System.err.println("获取配置地址属性：" + unreverseKey);
			return false;
		}
		if (page.getConfigUrl() == null) {
			System.err.println("配置地址为空：" + unreverseKey);
			return false;
		}
		// 获取配置项
		UrlPathMatch ids = baseUrlAttr.match(unreverseKey);
		if (ids == null) {
			System.err.println(unreverseKey + " 解析失败：未匹配到配置地址");
			return false;
		}
		// UrlPathMatch.AttrIdsConfig adCfg = (AttrIdsConfig) ids.getNodeValue();
		String configUrl = ids.getConfigPath();
		page.setConfigUrl(new Utf8(configUrl));
		psg.setConfigUrl(new Utf8(configUrl));
		// a.BASE_URL,a.ROOT_SITE_ID,a.MEDIA_TYPE_ID,a.MEDIA_LEVEL_ID,a.TOPIC_TYPE_ID,a.area_id
		Utf8 mr = Mark.ROOTID_MARK.checkMark(page);
		if (mr != null) {
			psg.setRootSiteId(Long.parseLong(mr.toString()));
		}
		mr = Mark.MEDIATYPEID_MARK.checkMark(page);
		if (mr != null) {
			psg.setMediaTypeId(Long.parseLong(mr.toString()));
		}
		mr = Mark.MEDIALEVELID_MARK.checkMark(page);
		if (mr != null) {
			psg.setMediaLevelId(Long.parseLong(mr.toString()));
		}
		mr = Mark.TOPICID_MARK.checkMark(page);
		if (mr != null) {
			psg.setTopicTypeId(Long.parseLong(mr.toString()));
		}
		mr = Mark.AREAID_MARK.checkMark(page);
		if (mr != null) {
			psg.setAreaId(Long.parseLong(mr.toString()));
		}
		// psg.setRootSiteId(adCfg.rootSiteId);
		// psg.setMediaTypeId(adCfg.mediaTypeId);
		// psg.setMediaLevelId(adCfg.mediaLevelId);
		// psg.setTopicTypeId(adCfg.topicTypeId);
		// psg.setAreaId(adCfg.areaId);
		TopicTypeRule topicTypeRule = topicTypeRules.get(psg.getTopicTypeId());
		if (topicTypeRule != null) {
			topicTypeRule.parse(unreverseKey, page, psg, util);
		}

		return true;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.conf = arg0;
		util = new ParseUtil(conf);
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

}
