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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * Thread safe implementation to connect to a HBase table.
 * 
 */
public class HBaseTableConnection implements HTableInterface {
	/*
	 * The current implementation uses ThreadLocal HTable instances. It keeps track of the floating instances in order to correctly flush
	 * and close the connection when it is closed. HBase itself provides a utility called HTablePool for maintaining a pool of tables, but
	 * there are still some drawbacks that are only solved in later releases.
	 */
	private final Configuration conf;
	private final ThreadLocal<HTable> tables;
	private final BlockingQueue<HTable> pool = new LinkedBlockingQueue<HTable>();
	private final boolean autoflush;
	private final String tableName;
	private HTable table = null;

	/**
	 * Instantiate new connection.
	 * 
	 * @param conf
	 * @param tableName
	 * @param autoflush
	 * @throws IOException
	 */
	public HBaseTableConnection(Configuration conf, String tableName, boolean autoflush) throws IOException {
		this.conf = conf;
		this.tableName = tableName;
		this.autoflush = autoflush;
		this.tables = new ThreadLocal<HTable>();
	}

	private HTable getTable() {

		if (table == null)
			table = tables.get();
		if (table == null) {
			try {
				table = new HTable(conf, tableName);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			table.setAutoFlush(autoflush);
			pool.add(table); // keep track
			tables.set(table);
		}
		return table;
	}

	@Override
	public void close() throws IOException {
		// Flush and close all instances.
		// (As an extra safeguard one might employ a shared variable i.e. 'closed'
		// in order to prevent further table creation but for now we assume that
		// once close() is called, clients are no longer using it).
		for (HTable table : pool) {
			table.flushCommits();
			table.close();
		}
	}

	@Override
	public byte[] getTableName() {
		return Bytes.toBytes(tableName);
	}

	@Override
	public Configuration getConfiguration() {
		return conf;
	}

	@Override
	public boolean isAutoFlush() {
		return autoflush;
	}

	/**
	 * getStartEndKeys provided by {@link HTable} but not {@link HTableInterface}.
	 * 
	 * @see HTable#getStartEndKeys()
	 */
	public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
		return getTable().getStartEndKeys();
	}

	public Pair<byte[][], byte[][]> getInRangeEndKeys(final byte[] startKey, final byte[] endKey) throws IOException {
		List<HRegionLocation> regions = getRegionsInRange(startKey, endKey);
		final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
		final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());
		for (HRegionLocation region : regions) {
			startKeyList.add(region.getRegionInfo().getStartKey());
			endKeyList.add(region.getRegionInfo().getEndKey());
		}
		return new Pair<byte[][], byte[][]>(startKeyList.toArray(new byte[startKeyList.size()][]), endKeyList.toArray(new byte[endKeyList
				.size()][]));
	}

	/**
	 * getRegionLocation provided by {@link HTable} but not {@link HTableInterface}.
	 * 
	 * @see HTable#getRegionLocation(byte[])
	 */
	public HRegionLocation getRegionLocation(final byte[] bs) throws IOException {
		return getTable().getRegionLocation(bs);
	}

	public List<HRegionLocation> getRegionsInRange(final byte[] startKey, final byte[] endKey) throws IOException {
		return getTable().getRegionsInRange(startKey, endKey);
	}

	@Override
	public HTableDescriptor getTableDescriptor() throws IOException {
		return getTable().getTableDescriptor();
	}

	@Override
	public boolean exists(Get get) throws IOException {
		return getTable().exists(get);
	}

	@Override
	public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
		return getTable().batch(actions);
	}

	@Override
	public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
		getTable().batch(actions, results);
	}

	@Override
	public Result get(Get get) throws IOException {
		return getTable().get(get);
	}

	@Override
	public Result[] get(List<Get> gets) throws IOException {
		return getTable().get(gets);
	}

	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
		return getTable().getRowOrBefore(row, family);
	}

	@Override
	public ResultScanner getScanner(Scan scan) throws IOException {
		return getTable().getScanner(scan);
	}

	@Override
	public ResultScanner getScanner(byte[] family) throws IOException {
		return getTable().getScanner(family);
	}

	@Override
	public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
		return getTable().getScanner(family, qualifier);
	}

	@Override
	public void put(Put put) throws IOException {
		getTable().put(put);
	}

	@Override
	public void put(List<Put> puts) throws IOException {
		getTable().put(puts);
	}

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
		return getTable().checkAndPut(row, family, qualifier, value, put);
	}

	@Override
	public void delete(Delete delete) throws IOException {
		getTable().delete(delete);
	}

	@Override
	public void delete(List<Delete> deletes) throws IOException {
		getTable().delete(deletes);

	}

	@Override
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
		return getTable().checkAndDelete(row, family, qualifier, value, delete);
	}

	@Override
	public Result increment(Increment increment) throws IOException {
		return getTable().increment(increment);
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
		return getTable().incrementColumnValue(row, family, qualifier, amount);
	}

	@Override
	public void flushCommits() throws IOException {
		for (HTable table : pool) {
			table.flushCommits();
		}
	}

	@Override
	public Result append(Append append) throws IOException {
		return getTable().append(append);
	}

	@Override
	public long getWriteBufferSize() {
		return getTable().getWriteBufferSize();
	}

	@Override
	public void mutateRow(RowMutations rm) throws IOException {
		getTable().mutateRow(rm);
	}

	@Override
	public void setAutoFlush(boolean autoFlush) {
		getTable().setAutoFlush(autoFlush);
	}

	@Override
	public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
		getTable().setAutoFlush(autoFlush, clearBufferOnFail);
	}

	@Override
	public void setWriteBufferSize(long writeBufferSize) throws IOException {
		getTable().setWriteBufferSize(writeBufferSize);
	}

	@Override
	public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback) throws IOException, InterruptedException {
		return getTable().batchCallback(actions, callback);
	}

	@Override
	public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback) throws IOException,
			InterruptedException {
		getTable().batchCallback(actions, results, callback);
	}

	@Override
	public CoprocessorRpcChannel coprocessorService(byte[] row) {
		return getTable().coprocessorService(row);
	}

	@Override
	public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Call<T, R> callable)
			throws ServiceException, Throwable {
		return getTable().coprocessorService(service, startKey, endKey, callable);
	}

	@Override
	public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Call<T, R> callable,
			Callback<R> callback) throws ServiceException, Throwable {
		getTable().coprocessorService(service, startKey, endKey, callable, callback);
	}

	@Override
	public Boolean[] exists(List<Get> gets) throws IOException {
		return getTable().exists(gets);
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
		return getTable().incrementColumnValue(row, family, qualifier, amount, durability);
	}

	@Override
	public TableName getName() {
		return getTable().getName();
	}

	@Override
	public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3, boolean arg4) throws IOException {
		return getTable().incrementColumnValue(arg0, arg1, arg2, arg3, arg4);
	}

	@Override
	public void setAutoFlushTo(boolean arg0) {
		getTable().setAutoFlushTo(arg0);
	}

	@Override
	public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor methodDescriptor, Message request, byte[] startKey,
			byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
		return getTable().batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
	}

	@Override
	public <R extends Message> void batchCoprocessorService(MethodDescriptor arg0, Message arg1, byte[] arg2, byte[] arg3, R arg4,
			Callback<R> arg5) throws ServiceException, Throwable {
		getTable().batchCoprocessorService(arg0, arg1, arg2, arg3, arg4, arg5);
	}

	@Override
	public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, RowMutations rm)
			throws IOException {
		return getTable().checkAndMutate(row, family, qualifier, compareOp, value, rm);
	}

	@Override
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Delete delete)
			throws IOException {
		return getTable().checkAndDelete(row, family, qualifier, compareOp, value, delete);
	}

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Put put) throws IOException {
		return getTable().checkAndPut(row, family, qualifier, compareOp, value, put);
	}

	@Override
	public boolean[] existsAll(List<Get> arg0) throws IOException {
		return getTable().existsAll(arg0);
	}
}
