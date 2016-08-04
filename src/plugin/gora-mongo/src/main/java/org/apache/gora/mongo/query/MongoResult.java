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

package org.apache.gora.mongo.query;

import java.io.IOException;

import org.apache.gora.mongo.store.MongoStore;
import org.apache.gora.mongo.util.MongoObjectInterface;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.hadoop.hbase.client.Result;
import org.bson.Document;

import com.mongodb.client.MongoCursor;

/**
 * Base class for {@link Result} implementations for HBase.
 */
public class MongoResult<K, T extends PersistentBase> extends ResultBase<K, T> {
	public MongoResult(MongoStore<K, T> dataStore, Query<K, T> query) {
		super(dataStore, query);
	}

	private MongoCursor<Document> scanner;

	@Override
	public MongoStore<K, T> getDataStore() {
		return (MongoStore<K, T>) super.getDataStore();
	}

	protected void readNext(Document result) throws IOException {
		key = MongoObjectInterface.fromString(getKeyClass(),
				result.getString(((MongoStore<K, T>) super.getDataStore()).mapping.primaryColumn));
		persistent = getDataStore().newInstance(result, query.getFields());
	}

	// do not clear object in scanner result
	@Override
	protected void clear() {
	}

	@Override
	public boolean nextInner() throws IOException {
		Document result = scanner.next();
		if (result == null) {
			return false;
		}
		readNext(result);
		return true;
	}

	@Override
	public void close() throws IOException {
		scanner.close();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO: if limit is set, we know how far we have gone
		return 0;
	}

}
