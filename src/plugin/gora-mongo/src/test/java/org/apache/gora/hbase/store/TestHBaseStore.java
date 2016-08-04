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

package org.apache.gora.hbase.store;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.hbase.GoraHBaseTestDriver;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test case for HBaseStore.
 */
public class TestHBaseStore extends DataStoreTestBase {

	private Configuration conf;

	static {
		setTestDriver(new GoraHBaseTestDriver());
	}

	@Override
	public void setUp() throws Exception {
		super.setUp();
		conf = getTestDriver().getHbaseUtil().getConfiguration();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DataStore<String, Employee> createEmployeeDataStore() throws IOException {
		return DataStoreFactory.createDataStore(HBaseStore.class, String.class, Employee.class, conf);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DataStore<String, WebPage> createWebPageDataStore() throws IOException {
		return DataStoreFactory.createDataStore(HBaseStore.class, String.class, WebPage.class, conf);
	}

	public GoraHBaseTestDriver getTestDriver() {
		return (GoraHBaseTestDriver) testDriver;
	}

	@Override
	public void assertSchemaExists(String schemaName) throws Exception {
		HBaseAdmin admin = getTestDriver().getHbaseUtil().getHBaseAdmin();
		Assert.assertTrue(admin.tableExists(schemaName));
	}

	@Override
	public void assertPutArray() throws IOException {
		Get get = new Get(Bytes.toBytes("com.example/http"));
		HTable table = new HTable(HBaseConfiguration.create(), "WebPage");
		org.apache.hadoop.hbase.client.Result result = table.get(get);

		Assert.assertEquals(result.getFamilyMap(Bytes.toBytes("parsedContent")).size(), 4);
		Assert.assertTrue(Arrays.equals(result.getValue(Bytes.toBytes("parsedContent"), Bytes.toBytes(0)), Bytes.toBytes("example")));

		Assert.assertTrue(Arrays.equals(result.getValue(Bytes.toBytes("parsedContent"), Bytes.toBytes(3)), Bytes.toBytes("example.com")));
		table.close();
	}

	@Override
	public void assertPutBytes(byte[] contentBytes) throws IOException {
		HTable table = new HTable(HBaseConfiguration.create(), "WebPage");
		Get get = new Get(Bytes.toBytes("com.example/http"));
		org.apache.hadoop.hbase.client.Result result = table.get(get);

		byte[] actualBytes = result.getValue(Bytes.toBytes("content"), null);
		Assert.assertNotNull(actualBytes);
		Assert.assertTrue(Arrays.equals(contentBytes, actualBytes));
		table.close();
	}

	@Override
	public void assertPutMap() throws IOException {
		HTable table = new HTable(HBaseConfiguration.create(), "WebPage");
		Get get = new Get(Bytes.toBytes("com.example/http"));
		org.apache.hadoop.hbase.client.Result result = table.get(get);

		byte[] anchor2Raw = result.getValue(Bytes.toBytes("outlinks"), Bytes.toBytes("http://example2.com"));
		Assert.assertNotNull(anchor2Raw);
		String anchor2 = Bytes.toString(anchor2Raw);
		Assert.assertEquals("anchor2", anchor2);
		table.close();
	}

	@Override
	public void testQueryEndKey() throws IOException {
		// We need to skip this test since gora considers endRow inclusive, while its exclusinve for HBase.
		// TODO: We should raise an issue for HBase to allow us to specify if the endRow will be inclussive or exclusive.
	}

	@Override
	public void testQueryKeyRange() throws IOException {
		// We need to skip this test since gora considers endRow inclusive, while its exclusinve for HBase.
		// TODO: We should raise an issue for HBase to allow us to specify if the endRow will be inclussive or exclusive.
	}

	@Override
	public void testDeleteByQuery() throws IOException {
		// We need to skip this test since gora considers endRow inclusive, while its exclusinve for HBase.
		// TODO: We should raise an issue for HBase to allow us to specify if the endRow will be inclussive or exclusive.
	}

	public static void main(String[] args) throws Exception {
		TestHBaseStore test = new TestHBaseStore();
		test.setUpClass();
		test.setUp();

		test.testQuery();

		test.tearDown();
		test.tearDownClass();
	}
}
