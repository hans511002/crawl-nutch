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
public class WebPageIndex extends WebPage {
	private static final long serialVersionUID = 8663967303505909677L;

	// public static final Schema _SCHEMA = Schema
	// .parse("{\"type\":\"record\",\"name\":\"WebPageIndex\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"baseUrl\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"int\"},{\"name\":\"fetchTime\",\"type\":\"long\"},{\"name\":\"prevFetchTime\",\"type\":\"long\"},{\"name\":\"fetchInterval\",\"type\":\"int\"},{\"name\":\"retriesSinceFetch\",\"type\":\"int\"},{\"name\":\"modifiedTime\",\"type\":\"long\"},{\"name\":\"protocolStatus\",\"type\":{\"type\":\"record\",\"name\":\"ProtocolStatus\",\"fields\":[{\"name\":\"code\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"lastModified\",\"type\":\"long\"}]}},{\"name\":\"content\",\"type\":\"bytes\"},{\"name\":\"contentType\",\"type\":\"string\"},{\"name\":\"prevSignature\",\"type\":\"bytes\"},{\"name\":\"signature\",\"type\":\"bytes\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"parseStatus\",\"type\":{\"type\":\"record\",\"name\":\"ParseStatus\",\"fields\":[{\"name\":\"majorCode\",\"type\":\"int\"},{\"name\":\"minorCode\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}},{\"name\":\"score\",\"type\":\"float\"},{\"name\":\"reprUrl\",\"type\":\"string\"},{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"inlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"markers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}},{\"name\":\"crawltype\",\"type\":\"int\"},{\"name\":\"configUrl\",\"type\":\"string\"}]}");
	public static final Schema _SCHEMA = Schema
			.parse("{\"type\":\"record\",\"name\":\"WebPageIndex\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":"
					+ "[{\"name\":\"baseUrl\",\"type\":\"string\"}" + ",{\"name\":\"status\",\"type\":\"int\"}"
					+ ",{\"name\":\"fetchTime\",\"type\":\"long\"}" + ",{\"name\":\"prevFetchTime\",\"type\":\"long\"}"
					+ ",{\"name\":\"fetchInterval\",\"type\":\"int\"}" + ",{\"name\":\"retriesSinceFetch\",\"type\":\"int\"}"
					+ ",{\"name\":\"modifiedTime\",\"type\":\"long\"}"
					+ ",{\"name\":\"protocolStatus\",\"type\":{\"type\":\"record\",\"name\":\"ProtocolStatus\",\"fields\""
					+ ":[{\"name\":\"code\",\"type\":\"int\"}" + ",{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
					+ ",{\"name\":\"lastModified\",\"type\":\"long\"}]}}" + ",{\"name\":\"content\",\"type\":\"bytes\"}"
					+ ",{\"name\":\"contentType\",\"type\":\"string\"}" + ",{\"name\":\"prevSignature\",\"type\":\"bytes\"}"
					+ ",{\"name\":\"signature\",\"type\":\"bytes\"}" + ",{\"name\":\"title\",\"type\":\"string\"}"
					+ ",{\"name\":\"text\",\"type\":\"string\"}"
					+ ",{\"name\":\"parseStatus\",\"type\":{\"type\":\"record\",\"name\":\"ParseStatus\",\"fields\":"
					+ "[{\"name\":\"majorCode\",\"type\":\"int\"}" + ",{\"name\":\"minorCode\",\"type\":\"int\"}"
					+ ",{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}}"
					+ ",{\"name\":\"score\",\"type\":\"float\"}" + ",{\"name\":\"reprUrl\",\"type\":\"string\"}"
					+ ",{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"inlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"markers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
					+ ",{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}"
					+ ",{\"name\":\"crawlType\",\"type\":\"int\"}" + ",{\"name\":\"configUrl\",\"type\":\"string\"}]}");

	public static final String[] _ALL_FIELDS = { "baseUrl", "status", "fetchTime", "prevFetchTime", "fetchInterval", "retriesSinceFetch",
			"modifiedTime", "protocolStatus", "content", "contentType", "prevSignature", "signature", "title", "text", "parseStatus",
			"score", "reprUrl", "headers", "outlinks", "inlinks", "markers", "metadata", "crawlType", "configUrl" };
	// public static final String[] _ALL_FIELDS = { "baseUrl", "status", "fetchTime", "prevFetchTime", "fetchInterval", "retriesSinceFetch",
	// "modifiedTime", "protocolStatus", "content", "contentType", "prevSignature", "signature", "title", "text", "parseStatus",
	// "score", "reprUrl", "headers", "outlinks", "inlinks", "markers", "metadata", "crawlType", "configUrl" };
	static {
		PersistentBase.registerFields(WebPageIndex.class, _ALL_FIELDS);
	}

	public WebPageIndex() {
		super(new StateManagerImpl());
	}

	public WebPageIndex(WebPage page) {
		this(page, true);
	}

	public WebPageIndex(WebPage page, boolean copyMapValue) {
		super(new StateManagerImpl());
		int index = 0;
		this.put(index++, page.baseUrl);
		this.put(index++, page.status);
		this.put(index++, page.fetchTime);
		this.put(index++, page.prevFetchTime);
		this.put(index++, page.fetchInterval);

		this.put(index++, page.retriesSinceFetch);
		this.put(index++, page.modifiedTime);
		this.put(index++, page.protocolStatus);
		this.put(index++, page.content);
		this.put(index++, page.contentType);

		this.put(index++, page.prevSignature);
		this.put(index++, page.signature);
		this.put(index++, page.title);
		this.put(index++, page.text);
		this.put(index++, page.parseStatus);

		this.put(index++, page.score);
		this.put(index++, page.reprUrl);
		this.put(index++, page.headers);
		this.put(index++, page.outlinks);
		this.put(index++, page.inlinks);

		this.put(index++, page.markers);
		this.put(index++, page.metadata);
		if (copyMapValue) {
			if (this.headers instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.headers).states();
				for (Utf8 key : headers.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.outlinks instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.outlinks).states();
				for (Utf8 key : outlinks.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.inlinks instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.inlinks).states();
				for (Utf8 key : inlinks.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.markers instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.markers).states();
				for (Utf8 key : markers.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
			if (this.metadata instanceof StatefulMap) {
				Map<Utf8, State> states = ((StatefulMap) this.metadata).states();
				for (Utf8 key : metadata.keySet()) {
					if (states.get(key) != State.DELETED)
						states.put(key, State.DIRTY);
				}
			}
		}
		this.put(index++, page.crawlType);
		this.put(index++, page.configUrl);
	}

	public WebPageIndex(StateManager stateManager) {
		super(stateManager);
	}

	public WebPageIndex newInstance(StateManager stateManager) {
		return new WebPageIndex(stateManager);
	}

	public Schema getSchema() {
		return _SCHEMA;
	}

}
