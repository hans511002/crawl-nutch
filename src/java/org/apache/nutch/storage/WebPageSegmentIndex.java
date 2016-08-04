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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;

@SuppressWarnings("all")
public class WebPageSegmentIndex extends WebPageSegment {
	public static final Schema _SCHEMA = Schema
			.parse("{\"type\":\"record\",\"name\":\"WebPageSegmentIndex\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":["
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

	// public static final String[] _ALL_FIELDS = { "baseUrl", "configUrl", "score", "fetchTime", "parseTime", "dataTime", "title",
	// "rootSiteId", "mediaTypeId", "mediaLevelId", "topicTypeId", "policticTypeId", "areaId", "extendInfoAttrs", "segMentAttr",
	// "segMentCnt", "segMentSign", "segMentKeys", "unionUrls", "marker" };
	static {
		PersistentBase.registerFields(WebPageSegmentIndex.class, _ALL_FIELDS);
	}

	public WebPageSegmentIndex(WebPageSegment wps) {
		this(wps, true);
	}

	public WebPageSegmentIndex(WebPageSegment page, boolean copyMapValue) {
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

	public WebPageSegmentIndex() {
		super();
	}

	public WebPageSegmentIndex(StateManager stateManager) {
		super(stateManager);
	}

	public WebPageSegmentIndex newInstance(StateManager stateManager) {
		return new WebPageSegmentIndex(stateManager);
	}

}
