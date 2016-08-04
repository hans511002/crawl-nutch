/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.storage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.StatefulMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.crawl.NutchConstant;
import org.w3c.dom.Node;

@SuppressWarnings("all")
public class WebPageSegment extends PersistentBase implements java.io.Serializable {
	private static final long serialVersionUID = 1555774316289913985L;
	public static final Schema _SCHEMA = Schema
			.parse("{\"type\":\"record\",\"name\":\"WebPageSegment\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":["
					+ "{\"name\":\"baseUrl\",\"type\":\"string\"}" + ",{\"name\":\"configUrl\",\"type\":\"string\"}"
					+ ",{\"name\":\"score\",\"type\":\"float\"}" + ",{\"name\":\"fetchTime\",\"type\":\"long\"}"
					+ ",{\"name\":\"parseTime\",\"type\":\"long\"}" + ",{\"name\":\"dataTime\",\"type\":\"long\"}"
					+ ",{\"name\":\"title\",\"type\":\"string\"}" + ",{\"name\":\"rootSiteId\",\"type\":\"long\"}"
					+ ",{\"name\":\"mediaTypeId\",\"type\":\"long\"}" + ",{\"name\":\"mediaLevelId\",\"type\":\"long\"}"
					+ ",{\"name\":\"topicTypeId\",\"type\":\"long\"}" + ",{\"name\":\"policticTypeId\",\"type\":\"long\"}"
					+ ",{\"name\":\"areaId\",\"type\":\"long\"}"
					+ ",{\"name\":\"extendInfoAttrs\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"segMentAttr\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"segMentCnt\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"segMentSign\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}"
					+ ",{\"name\":\"segMentKeyWord\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}"
					+ ",{\"name\":\"unionUrls\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}"
					+ ",{\"name\":\"marker\",\"type\":\"string\"}]}");
	public static final Utf8 dataTimeColName = new Utf8("dt");// 内置的数据时间字段
	public static final Utf8 keyworksColName = new Utf8("keys");// 内置的关键字字段
	public static final Utf8 descriptionColName = new Utf8("desc");// 内置的描述字段
	public static final Utf8 titleColName = new Utf8("title");// 内置的标题字段
	public static final Utf8 topicTypeIdColName = new Utf8("topic");// 内置的栏目字段
	public static final Utf8 subLinkColName = new Utf8("subValMap");
	public static final String SegmentColParsePerentKey = "segment.col.right.perent";

	public static enum Field {
		BASE_URL(0, "baseUrl"), CONFIGURL(1, "configUrl"), SCORE(2, "score"), FETCH_TIME(3, "fetchTime"), PARSETIME(4, "parseTime"), DATATIME(
				5, "dataTime"), TITLE(6, "title"), ROOTSITEID(7, "rootSiteId"), MEDIATYPEID(8, "mediaTypeId"), MEDIALEVELID(9,
				"mediaLevelId"), TOPICTYPEID(10, "topicTypeId"), POLICTICTYPEID(11, "policticTypeId"), AREAID(12, "areaId"), extendInfoAttrs(
				13, "extendInfoAttrs"), SEGMENTATTR(14, "segMentAttr"), SEGMENTCNT(15, "segMentCnt"), SEGMENTSIGN(16, "segMentSign"), SEGMENTKEYS(
				17, "segMentKeys"), UNIONURLS(18, "unionUrls"), MARKER(19, "marker");
		private int index;
		private String name;

		Field(int index, String name) {
			this.index = index;
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public String getName() {
			return name;
		}

		public String toString() {
			return name;
		}
	};

	public static final String[] _ALL_FIELDS = { "baseUrl", "configUrl", "score", "fetchTime", "parseTime", "dataTime", "title",
			"rootSiteId", "mediaTypeId", "mediaLevelId", "topicTypeId", "policticTypeId", "areaId", "extendInfoAttrs", "segMentAttr",
			"segMentCnt", "segMentSign", "segMentKeys", "unionUrls", "marker" };
	static {
		PersistentBase.registerFields(WebPageSegment.class, _ALL_FIELDS);
	}
	Utf8 baseUrl;// 地址
	Utf8 configUrl;// 配置地址
	float score;// 评分
	long fetchTime = 0;// 抓取时间
	long parseTime = 0;// 解析时间
	long dataTime = 0;// 数据时间
	Utf8 title;
	long rootSiteId = 0;// 配置根地址ID
	long mediaTypeId = 0;// 媒体类型ID
	long mediaLevelId = 0;// 媒体级别ID
	long topicTypeId = 0;// 栏目类型ID
	long policticTypeId = -1;// 信息正负面ID
	long areaId = 0;// 地域ID
	Map<Utf8, Utf8> extendInfoAttrs;// 信息属性类型ID
	Map<Utf8, Utf8> segMentAttr;// 内容要素二级属性 标签名=segmeng_col_segmeng_attr_col:value
	Map<Utf8, Utf8> segMentCnt;// 内容要素 segmeng_col,segmentContent
	Map<Utf8, Utf8> segMentSign;// 内容签名
	Map<Utf8, Utf8> segMentKeys;// 分词关键字
	Map<Utf8, Utf8> unionUrls;// 内容相关联的URL url,title
	Utf8 marker;

	public String content = null;
	public Node rootNode = null;
	public HashMap<String, WebPageSegment> subWps = null;// 只用于解析数据到子页面

	public void setContent(WebPage page) throws IOException {
		if (this.content == null) {
			try {
				content = new String(page.getContent().array(), "utf-8");
			} catch (UnsupportedEncodingException e) {
				throw new IOException(e);
			}
		}
	}

	public WebPageSegment() {
		this(new StateManagerImpl());
	}

	public WebPageSegment(StateManager stateManager) {
		super(stateManager);
		extendInfoAttrs = new StatefulHashMap<Utf8, Utf8>();
		segMentAttr = new StatefulHashMap<Utf8, Utf8>();
		segMentCnt = new StatefulHashMap<Utf8, Utf8>();
		segMentSign = new StatefulHashMap<Utf8, Utf8>();
		segMentKeys = new StatefulHashMap<Utf8, Utf8>();
	}

	public WebPageSegment newInstance(StateManager stateManager) {
		return new WebPageSegment(stateManager);
	}

	public WebPageSegment(WebPageSegmentIndex page, boolean copyMapValue) {
		super(new StateManagerImpl());
		int index = 0;
		this.put(index++, page.baseUrl);
		this.put(index++, page.configUrl);
		this.put(index++, page.score);
		this.put(index++, page.fetchTime);
		this.put(index++, page.parseTime);

		this.put(index++, page.dataTime);
		this.put(index++, page.title);
		this.put(index++, page.rootSiteId);
		this.put(index++, page.mediaTypeId);
		this.put(index++, page.mediaLevelId);

		this.put(index++, page.topicTypeId);
		this.put(index++, page.policticTypeId);
		this.put(index++, page.areaId);
		this.put(index++, page.extendInfoAttrs);
		this.put(index++, page.segMentAttr);

		this.put(index++, page.segMentCnt);
		this.put(index++, page.segMentSign);
		this.put(index++, page.segMentKeys);
		this.put(index++, page.unionUrls);
		this.put(index++, page.marker);

		if (copyMapValue) {
			if (this.extendInfoAttrs instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.extendInfoAttrs).states();
				for (Utf8 key : extendInfoAttrs.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.segMentAttr instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.segMentAttr).states();
				for (Utf8 key : segMentAttr.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.segMentCnt instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.segMentCnt).states();
				for (Utf8 key : segMentCnt.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.segMentSign instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.segMentSign).states();
				for (Utf8 key : segMentSign.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.segMentKeys instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.segMentKeys).states();
				for (Utf8 key : segMentKeys.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.unionUrls instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.unionUrls).states();
				for (Utf8 key : unionUrls.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
		}
	}

	public Schema getSchema() {
		return _SCHEMA;
	}

	public Object get(int _field) {
		switch (_field) {
		case 0:
			return baseUrl;
		case 1:
			return configUrl;
		case 2:
			return score;
		case 3:
			return fetchTime;
		case 4:
			return parseTime;
		case 5:
			return dataTime;
		case 6:
			return title;
		case 7:
			return rootSiteId;
		case 8:
			return mediaTypeId;
		case 9:
			return mediaLevelId;
		case 10:
			return topicTypeId;
		case 11:
			return policticTypeId;
		case 12:
			return areaId;
		case 13:
			return extendInfoAttrs;
		case 14:
			return segMentAttr;
		case 15:
			return segMentCnt;
		case 16:
			return segMentSign;
		case 17:
			return segMentKeys;
		case 18:
			return unionUrls;
		case 19:
			return marker;
		default:
			throw new AvroRuntimeException("Bad index");
		}
	}

	@SuppressWarnings(value = "unchecked")
	public void put(int _field, Object _value) {
		if (isFieldEqual(_field, _value))
			return;
		getStateManager().setDirty(this, _field);
		switch (_field) {
		case 0:
			baseUrl = (Utf8) _value;
			break;
		case 1:
			configUrl = (Utf8) _value;
			break;
		case 2:
			score = (Float) _value;
			break;
		case 3:
			fetchTime = (Long) _value;
			break;
		case 4:
			parseTime = (Long) _value;
			break;
		case 5:
			dataTime = (Long) _value;
			break;
		case 6:
			title = (Utf8) _value;
			break;
		case 7:
			rootSiteId = (Long) _value;
			break;
		case 8:
			mediaTypeId = (Long) _value;
			break;
		case 9:
			mediaLevelId = (Long) _value;
			break;
		case 10:
			topicTypeId = (Long) _value;
			break;
		case 11:
			policticTypeId = (Long) _value;
			break;
		case 12:
			areaId = (Long) _value;
			break;
		case 13:
			extendInfoAttrs = (Map<Utf8, Utf8>) _value;
			break;
		case 14:
			segMentAttr = (Map<Utf8, Utf8>) _value;
			break;
		case 15:
			segMentCnt = (Map<Utf8, Utf8>) _value;
			break;
		case 16:
			segMentSign = (Map<Utf8, Utf8>) _value;
			break;
		case 17:
			segMentKeys = (Map<Utf8, Utf8>) _value;
			break;
		case 18:
			unionUrls = (Map<Utf8, Utf8>) _value;
			break;
		case 19:
			marker = (Utf8) _value;
			break;
		default:
			throw new AvroRuntimeException("Bad index");
		}
	}

	public Utf8 getBaseUrl() {
		return (Utf8) get(0);
	}

	public void setBaseUrl(Utf8 value) {
		put(0, value);
	}

	public Utf8 getConfigUrl() {
		return (Utf8) get(1);
	}

	public void setConfigUrl(Utf8 configUrl) {
		put(1, configUrl);
	}

	public float getScore() {
		return (Float) get(2);
	}

	public void setScore(float score) {
		put(2, score);
	}

	public long getFetchTime() {
		return (Long) get(3);
	}

	public void setFetchTime(long fetchTime) {
		put(3, fetchTime);
	}

	public long getParseTime() {
		return (Long) get(4);
	}

	public void setParseTime(long parseTime) {
		put(4, parseTime);
	}

	public long getDataTime() {
		return (Long) get(5);
	}

	public void setDataTime(long dataTime) {
		put(5, dataTime);
	}

	public Utf8 getTitle() {
		return (Utf8) get(6);
	}

	public void setTitle(Utf8 title) {
		put(6, title);
	}

	public long getRootSiteId() {
		return (Long) get(7);
	}

	public void setRootSiteId(long rootSiteId) {
		put(7, rootSiteId);
	}

	public long getMediaTypeId() {
		return (Long) get(8);
	}

	public void setMediaTypeId(long mediaTypeId) {
		put(8, mediaTypeId);
	}

	public long getMediaLevelId() {
		return (Long) get(9);
	}

	public void setMediaLevelId(long mediaLevelId) {
		put(9, mediaLevelId);
	}

	public long getTopicTypeId() {
		return (Long) get(10);
	}

	public void setTopicTypeId(long topicTypeId) {
		put(10, topicTypeId);
	}

	public long getPolicticTypeId() {
		return (Long) get(11);
	}

	public void setPolicticTypeId(long policticTypeId) {
		put(11, policticTypeId);
	}

	public long getAreaId() {
		return (Long) get(12);
	}

	public void setAreaId(long areaId) {
		put(12, areaId);
	}

	public Map<Utf8, Utf8> getExtendInfoAttrs() {
		return (Map<Utf8, Utf8>) get(13);
	}

	public Utf8 getExtendInfoAttrs(Utf8 key) {
		if (extendInfoAttrs == null) {
			return null;
		}
		return extendInfoAttrs.get(key);
	}

	public void putToExtendInfoAttrs(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 13);
		extendInfoAttrs.put(key, value);
	}

	public Utf8 removeFromExtendInfoAttrs(Utf8 key) {
		if (extendInfoAttrs == null) {
			return null;
		}
		getStateManager().setDirty(this, 13);
		return extendInfoAttrs.remove(key);
	}

	public Map<Utf8, Utf8> getSegMentAttr() {
		return (Map<Utf8, Utf8>) get(14);
	}

	public Utf8 getSegMentAttr(Utf8 key) {
		if (segMentAttr == null) {
			return null;
		}
		return segMentAttr.get(key);
	}

	public void putToSegMentAttr(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 14);
		segMentAttr.put(key, value);
	}

	public Utf8 removeFromSegMentAttr(Utf8 key) {
		if (segMentAttr == null) {
			return null;
		}
		getStateManager().setDirty(this, 14);
		return segMentAttr.remove(key);
	}

	public Map<Utf8, Utf8> getSegMentCnt() {
		return (Map<Utf8, Utf8>) get(15);
	}

	public Utf8 getSegMentCnt(Utf8 key) {
		if (segMentCnt == null) {
			return null;
		}
		return segMentCnt.get(key);
	}

	public void putToSegMentCnt(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 15);
		segMentCnt.put(key, value);
	}

	public Utf8 removeFromSegMentCnt(Utf8 key) {
		if (segMentCnt == null) {
			return null;
		}
		getStateManager().setDirty(this, 15);
		return segMentCnt.remove(key);
	}

	public Map<Utf8, Utf8> getSegMentSign() {
		return (Map<Utf8, Utf8>) get(16);
	}

	public Utf8 getSegMentSign(Utf8 key) {
		if (segMentSign == null) {
			return null;
		}
		return segMentSign.get(key);
	}

	public void putToSegMentSign(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 16);
		segMentSign.put(key, value);
	}

	public Utf8 removeFromSegMentSign(Utf8 key) {
		if (segMentSign == null) {
			return null;
		}
		getStateManager().setDirty(this, 16);
		return segMentSign.remove(key);
	}

	public Map<Utf8, Utf8> getSegMentKeys() {
		return (Map<Utf8, Utf8>) get(17);
	}

	public Utf8 getSegMentKeys(Utf8 key) {
		if (segMentKeys == null) {
			return null;
		}
		return segMentKeys.get(key);
	}

	public void putToSegMentKeys(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 17);
		segMentKeys.put(key, value);
	}

	public Utf8 removeFromSegMentKeys(Utf8 key) {
		if (segMentKeys == null) {
			return null;
		}
		getStateManager().setDirty(this, 17);
		return segMentKeys.remove(key);
	}

	public Map<Utf8, Utf8> getUnionUrls() {
		return (Map<Utf8, Utf8>) get(18);
	}

	public Utf8 getUnionUrls(Utf8 key) {
		if (unionUrls == null) {
			return null;
		}
		return unionUrls.get(key);
	}

	public void putToUnionUrls(Utf8 key, Utf8 value) {
		getStateManager().setDirty(this, 18);
		unionUrls.put(key, value);
	}

	public Utf8 removeFromUnionUrls(Utf8 key) {
		if (unionUrls == null) {
			return null;
		}
		getStateManager().setDirty(this, 18);
		return unionUrls.remove(key);
	}

	public Utf8 getMarker() {
		return (Utf8) get(19);
	}

	public void setMarker(Utf8 value) {
		put(19, value);
	}

	public static <inV extends Persistent, K, V> void initMapperJob(Job job, Collection<WebPageSegment.Field> fields,
			Class<inV> inValueClass, Class<K> outKeyClass, Class<V> outValueClass,
			Class<? extends GoraMapper<String, inV, K, V>> mapperClass, Class<? extends Partitioner<K, V>> partitionerClass,
			boolean reuseObjects) throws ClassNotFoundException, IOException {
		DataStore<String, inV> store = StorageUtils.createWebStore(job.getConfiguration(), String.class, inValueClass);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, inV> query = store.newQuery();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(job.getConfiguration());
		}
		query.setFields(StorageUtils.toStringArray(fields));
		String startKey = job.getConfiguration().get(NutchConstant.stepHbaseStartRowKey, null);
		String endKey = job.getConfiguration().get(NutchConstant.stepHbaseEndRowKey, null);
		if (startKey != null && !startKey.trim().equals("")) {
			System.out.println("set hbase query startKey=" + startKey);
			query.setStartKey(startKey);
		}
		if (endKey != null && !endKey.trim().equals("")) {
			System.out.println("set hbase query endKey=" + endKey);
			query.setEndKey(endKey);
		}
		GoraMapper.initMapperJob(job, query, store, outKeyClass, outValueClass, mapperClass, partitionerClass, reuseObjects);
		GoraOutputFormat.setOutput(job, store, true);
	}

}
