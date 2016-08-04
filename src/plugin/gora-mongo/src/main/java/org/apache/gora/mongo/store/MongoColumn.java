/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.mongo.store;

import org.apache.gora.mongo.store.MongoTypeInterface.JdbcType;

public class MongoColumn {

	private String name;
	private JdbcType jdbcType;
	private boolean isPrimaryKey;

	public String toString() {
		return "name:" + name + " isPrimaryKey:" + isPrimaryKey + " jdbcType:" + jdbcType + "\n";
	}

	public MongoColumn() {
	}

	public MongoColumn(String name) {
		this.name = name;
	}

	public MongoColumn(String name, boolean isPrimaryKey, JdbcType jdbcType) {
		this.name = name;
		this.isPrimaryKey = isPrimaryKey;
		this.jdbcType = jdbcType;
	}

	public MongoColumn(String name, boolean isPrimaryKey) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JdbcType getJdbcType() {
		return jdbcType;
	}

	public void setJdbcType(JdbcType jdbcType) {
		this.jdbcType = jdbcType;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}
}
