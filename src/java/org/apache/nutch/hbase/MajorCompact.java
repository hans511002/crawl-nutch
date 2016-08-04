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
package org.apache.nutch.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MajorCompact extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(MajorCompact.class);
	private Configuration conf;

	public MajorCompact() {

	}

	public MajorCompact(Configuration conf) {
		setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		throw new Exception("run(Map<String, Object> args) not support");
	}

	public int run(String[] args) {
		try {
			if (args.length < 1) {
				System.err.println("Usage: MajorCompact <tablename>");
				return -1;
			}
			HBaseAdmin admin = new HBaseAdmin(this.getConf());
			List<String> tables = new ArrayList<String>();
			for (int i = 0; i < args.length; i++) {
				if (!admin.tableExists(args[i])) {
					LOG.error("table " + args[i] + " is not extits");
					continue;
				}
				tables.add(args[i]);
			}
			if (tables.size() == 0) {
				return -1;
			}
			for (int i = 0; i < tables.size(); i++) {
				admin.majorCompact(tables.get(i));
				admin.compact(tables.get(i));
			}
			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		final int res = ToolRunner.run(NutchConfiguration.create(), new MajorCompact(), args);
		System.exit(res);
	}
}
