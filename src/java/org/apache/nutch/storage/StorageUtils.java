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
import java.util.Collection;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.metadata.Nutch;

/**
 * Entry point to Gora store/mapreduce functionality. Translates the concept of "crawlid" to the corresponding Gora
 * support.
 */
public class StorageUtils {

	/**
	 * Creates a store for the given persistentClass. Currently supports Webpage and Host stores.
	 * 
	 * @param conf
	 * @param keyClass
	 * @param persistentClass
	 * @return
	 * @throws ClassNotFoundException
	 * @throws GoraException
	 */
	@SuppressWarnings("unchecked")
	public static <K, V extends Persistent> DataStore<K, V> createWebStore(Configuration conf, Class<K> keyClass,
			Class<V> persistentClass) throws ClassNotFoundException, GoraException {

		String schema = null;
		if (WebPage.class.equals(persistentClass)) {
			schema = conf.get("storage.schema.webpage", "webpage");
		} else if (Host.class.equals(persistentClass)) {
			schema = conf.get("storage.schema.host", "host");
		} else if (WebPageIndex.class.equals(persistentClass)) {
			schema = conf.get("storage.schema.webpageindex", "webpageindex");
		} else if (WebPageSegment.class.equals(persistentClass)) {
			schema = conf.get("storage.schema.webpagesegment", "webpagesegment");
		} else if (WebPageSegmentIndex.class.equals(persistentClass)) {
			schema = conf.get("storage.schema.WebPageSegmentIndex", "webpagesegmentindex");
		} else {
			throw new UnsupportedOperationException("Unable to create store for class " + persistentClass);
		}
		String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");

		if (!crawlId.isEmpty()) {
			conf.set("schema.prefix", crawlId + "_");
		} else {
			conf.set("schema.prefix", "");
		}

		Class<? extends DataStore<K, V>> dataStoreClass = (Class<? extends DataStore<K, V>>) getDataStoreClass(conf);
		return DataStoreFactory.createDataStore(dataStoreClass, keyClass, persistentClass, conf, schema);
	}

	@SuppressWarnings("unchecked")
	private static <K, V extends PersistentBase> Class<? extends DataStore<K, V>> getDataStoreClass(Configuration conf)
			throws ClassNotFoundException {
		return (Class<? extends DataStore<K, V>>) Class.forName(conf.get("storage.data.store.class",
				"org.apache.gora.sql.store.SqlStore"));
	}

	public static <inV extends Persistent, K, V> void initMapperJob(Job job, Collection<WebPage.Field> fields,
			Class<inV> inValueClass, Class<K> outKeyClass, Class<V> outValueClass,
			Class<? extends GoraMapper<String, inV, K, V>> mapperClass) throws ClassNotFoundException, IOException {
		initMapperJob(job, fields, inValueClass, outKeyClass, outValueClass, mapperClass, null, true);
	}

	public static <inV extends Persistent, K, V> void initMapperJob(Job job, Collection<WebPage.Field> fields,
			Class<inV> inValueClass, Class<K> outKeyClass, Class<V> outValueClass,
			Class<? extends GoraMapper<String, inV, K, V>> mapperClass,
			Class<? extends Partitioner<K, V>> partitionerClass) throws ClassNotFoundException, IOException {
		initMapperJob(job, fields, inValueClass, outKeyClass, outValueClass, mapperClass, partitionerClass, true);
	}

	public static <inV extends Persistent, K, V> void initMapperJob(Job job, Collection<WebPage.Field> fields,
			Class<inV> inValueClass, Class<K> outKeyClass, Class<V> outValueClass,
			Class<? extends GoraMapper<String, inV, K, V>> mapperClass,
			Class<? extends Partitioner<K, V>> partitionerClass, boolean reuseObjects) throws ClassNotFoundException,
			IOException {
		DataStore<String, inV> store = createWebStore(job.getConfiguration(), String.class, inValueClass);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, inV> query = store.newQuery();
		Configuration conf = job.getConfiguration();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(job.getConfiguration());
		}
		query.setFields(toStringArray(fields));
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
		GoraMapper.initMapperJob(job, query, store, outKeyClass, outValueClass, mapperClass, partitionerClass,
				reuseObjects);
		GoraOutputFormat.setOutput(job, store, true);
	}

	public static <inK, inV, outV extends Persistent> void initReducerJob(Job job, Class<outV> outValueClass,
			Class<? extends GoraReducer<inK, inV, String, outV>> reducerClass) throws ClassNotFoundException,
			GoraException {
		Configuration conf = job.getConfiguration();
		DataStore<String, outV> store = StorageUtils.createWebStore(conf, String.class, outValueClass);
		GoraReducer.initReducerJob(job, store, reducerClass);
		GoraOutputFormat.setOutput(job, store, true);
	}

	public static String[] toStringArray(Collection<?> fields) {
		String[] arr = new String[fields.size()];
		Object o[] = fields.toArray();
		for (int i = 0; i < arr.length; i++) {
			if (o[i] instanceof WebPage.Field) {
				arr[i] = ((WebPage.Field) o[i]).getName();
			} else if (o[i] instanceof WebPageSegment.Field) {
				arr[i] = ((WebPageSegment.Field) o[i]).getName();
			}
		}
		// Iterator<WebPage.Field> iter = (Iterator<Field>) fields.iterator();
		// for (int i = 0; i < arr.length; i++) {
		// arr[i] = iter.next().getName();
		// }
		return arr;
	}
	//
	// public static String[] toStringArray(Collection<WebPageSegment.Field> fields) {
	// String[] arr = new String[fields.size()];
	// Iterator<WebPageSegment.Field> iter = fields.iterator();
	// for (int i = 0; i < arr.length; i++) {
	// arr[i] = iter.next().getName();
	// }
	// return arr;
	// }
}
