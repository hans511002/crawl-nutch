/**
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
 */

package org.apache.nutch.parse.element;

// Hadoop imports
import java.io.Serializable;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.plugin.Pluggable;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

/**
 * Interface used to limit which URLs enter Nutch. Used by the injector and the db updater.
 */

public abstract class SegMentParse implements FieldPluggable, Serializable, Pluggable, Configurable {
	private static final long serialVersionUID = 5189510149699356087L;
	/** The name of the extension point. */
	public final static String X_POINT_ID = SegMentParse.class.getName();
	public final static String contentType = "text/html";
	public static ParseUtil util = null;

	/*
	 * Interface for a filter that transforms a URL: it can pass the original URL through or "delete" the URL by returning null
	 */
	public abstract boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg) throws Exception;
}
