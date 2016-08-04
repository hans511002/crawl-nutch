/*
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
package org.apache.nutch.parse.json;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;

/**
 * This class is a heuristic link extractor for JavaScript files and
 * code snippets. The general idea of a two-pass regex matching comes from
 * Heritrix. Parts of the code come from OutlinkExtractor.java
 * by Stephan Strittmatter.
 *
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class JSONParser implements Parser {
  public static final Logger LOG = LoggerFactory.getLogger(JSONParser.class);


  private Configuration conf;

  /**
   * Set the {@link Configuration} object
   * @param url URL of the {@link WebPage} which is parsed
   * @param page {@link WebPage} object relative to the URL
   * @return parse the actual {@link Parse} object
   */
  @Override
  public Parse getParse(String url, WebPage page) {
    String type = TableUtil.toString(page.getContentType());
    if (type != null && !type.trim().equals("") && !type.toLowerCase().startsWith("application/json"))
      return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_INVALID_FORMAT, "Content not JSON: '" + type + "'", getConf());
    
    String json = new String(page.getContent().array());
    
    Parse parse = new Parse(json, "", new Outlink[0], ParseStatusUtils.STATUS_SUCCESS);
    return parse;
  }

  /**
   * Main method which can be run from command line with the plugin option.
   * The method takes two arguments e.g. o.a.n.parse.js.JSParseFilter file.js baseURL  
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
   
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Gets all the fields for a given {@link WebPage}
   * Many datastores need to setup the mapreduce job by specifying the fields
   * needed. All extensions that work on WebPage are able to specify what fields
   * they need.
   */
  @Override
  public Collection<WebPage.Field> getFields() {
    return null;
  }

}
