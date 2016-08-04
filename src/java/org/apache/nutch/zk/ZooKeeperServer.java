package org.apache.nutch.zk;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperServer implements Serializable {

	private static final long serialVersionUID = 6209696012310466381L;
	// ����һ��Zookeeperʵ���һ������ΪĿ���������ַ�Ͷ˿ڣ��ڶ�������ΪSession��ʱʱ�䣬�����Ϊ�ڵ�仯ʱ�Ļص�����
	private ZooKeeper zk = null;
	private String servers = null;
	public static int defaultTimeout = 180000;
	private MultiWatcher watch = new MultiWatcher(this);

	public ZooKeeperServer() {
	}

	public ZooKeeperServer(String servers) throws IOException {
		this(servers, defaultTimeout);
	}

	public ZooKeeperServer(String servers, int timeout) throws IOException {
		this.servers = servers;
		zk = new ZooKeeper(this.servers, timeout, watch);
	}

	public boolean connect(String servers, int timeout) {
		if (zk == null) {
			this.servers = servers;
			try {
				zk = new ZooKeeper(servers, timeout, watch);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	public boolean connect(String servers) {
		return connect(servers, defaultTimeout);
	}

	public boolean connect() {
		return connect(servers, defaultTimeout);
	}

	public ZooKeeper getZk() {
		return zk;
	}

	public MultiWatcher getWatch() {
		return this.watch;
	}

	/**
	 * The session id for this ZooKeeper client instance. The value returned is not valid until the client connects to a server and may
	 * change after a re-connect. This method is NOT thread safe
	 * 
	 * @return current session id
	 */
	public long getSessionId() {
		return zk.getSessionId();
	}

	/**
	 * The session password for this ZooKeeper client instance. The value returned is not valid until the client connects to a server and
	 * may change after a re-connect. This method is NOT thread safe
	 * 
	 * @return current session password
	 */
	public byte[] getSessionPasswd() {
		byte[] res = zk.getSessionPasswd();
		return res == null ? new byte[0] : res;
	}

	/**
	 * The negotiated session timeout for this ZooKeeper client instance. The value returned is not valid until the client connects to a
	 * server and may change after a re-connect. This method is NOT thread safe
	 * 
	 * @return current session timeout
	 */
	public int getSessionTimeout() {
		return zk.getSessionTimeout();
	}

	/**
	 * Add the specified scheme:auth information to this connection. This method is NOT thread safe
	 * 
	 * @param scheme
	 * @param auth
	 */
	public void addAuthInfo(String scheme, byte auth[]) {
		zk.addAuthInfo(scheme, auth);
	}

	/**
	 * Specify the default watcher for the connection (overrides the one specified during construction).
	 * 
	 * @param watcher
	 */
	public synchronized void register(Watcher watcher) {
		zk.register(watcher);
	}

	/**
	 * Close this client object. Once the client is closed, its session becomes invalid. All the ephemeral nodes in the ZooKeeper server
	 * associated with the session will be removed. The watches left on those nodes (and on their parents) will be triggered.
	 * 
	 * @throws InterruptedException
	 */
	public synchronized void close() throws InterruptedException {
		zk.close();
	}

	/**
	 * Create a node with the given path. The node data will be the given data, and node acl will be the given acl.
	 * <p>
	 * The flags argument specifies whether the created node will be ephemeral or not.
	 * <p>
	 * An ephemeral node will be removed by the ZooKeeper automatically when the session associated with the creation of the node expires.
	 * <p>
	 * The flags argument can also specify to create a sequential node. The actual path name of a sequential node will be the given path
	 * plus a suffix "i" where i is the current sequential number of the node. The sequence number is always fixed length of 10 digits, 0
	 * padded. Once such a node is created, the sequential number will be incremented by one.
	 * <p>
	 * If a node with the same actual path already exists in the ZooKeeper, a KeeperException with error code KeeperException.NodeExists
	 * will be thrown. Note that since a different actual path is used for each invocation of creating sequential node with the same path
	 * argument, the call will never throw "file exists" KeeperException.
	 * <p>
	 * If the parent node does not exist in the ZooKeeper, a KeeperException with error code KeeperException.NoNode will be thrown.
	 * <p>
	 * An ephemeral node cannot have children. If the parent node of the given path is ephemeral, a KeeperException with error code
	 * KeeperException.NoChildrenForEphemerals will be thrown.
	 * <p>
	 * This operation, if successful, will trigger all the watches left on the node of the given path by exists and getData API calls, and
	 * the watches left on the parent node by getChildren API calls.
	 * <p>
	 * If a node is created successfully, the ZooKeeper server will trigger the watches on the path left by exists calls, and the watches on
	 * the parent of the node by getChildren calls.
	 * <p>
	 * The maximum allowable size of the data array is 1 MB (1,048,576 bytes). Arrays larger than this will cause a KeeperExecption to be
	 * thrown.
	 * 
	 * @param path
	 *            the path for the node
	 * @param data
	 *            the initial data for the node
	 * @param acl
	 *            the acl for the node
	 * @param createMode
	 *            specifying whether the node to be created is ephemeral and/or sequential
	 * @return the actual path of the created node
	 * @throws KeeperException
	 *             if the server returns a non-zero error code
	 * @throws KeeperException.InvalidACLException
	 *             if the ACL is invalid, null, or empty
	 * @throws InterruptedException
	 *             if the transaction is interrupted
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
		return zk.create(path, data, acl, createMode);
	}

	/**
	 * The asynchronous version of create.
	 * 
	 * @see #create(String, byte[], List, CreateMode)
	 */

	public void create(final String path, byte data[], List<ACL> acl, CreateMode createMode, StringCallback cb, Object ctx) {
		zk.create(path, data, acl, createMode, cb, ctx);
	}

	/*
	 * ����һ�����Ŀ¼�ڵ� path, ������������ݣ� CreateMode ��ʶ��������ʽ��Ŀ¼�ڵ�,�ֱ��� PERSISTENT���־û�Ŀ¼�ڵ㣬���Ŀ¼�ڵ�洢����ݲ��ᶪʧ��
	 * PERSISTENT_SEQUENTIAL��˳���Զ���ŵ�Ŀ¼�ڵ㣬����Ŀ¼�ڵ���ݵ�ǰ�ѽ���ڵĽڵ����Զ��� 1�� Ȼ�󷵻ظ�ͻ����Ѿ��ɹ�������Ŀ¼�ڵ���
	 * EPHEMERAL����ʱĿ¼�ڵ㣬һ����������ڵ�Ŀͻ�����������Ͽ���Ҳ���� session ��ʱ�� ���ֽڵ�ᱻ�Զ�ɾ�� EPHEMERAL_SEQUENTIAL����ʱ�Զ���Žڵ�
	 */
	public void createTempNode(String path, String data) throws KeeperException, InterruptedException {
		zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	public void createPresistentNode(String path, String data) throws KeeperException, InterruptedException {
		if (data != null)
			zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		else
			zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	/**
	 * Delete the node with the given path. The call will succeed if such a node exists, and the given version matches the node's version
	 * (if the given version is -1, it matches any node's versions).
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if the nodes does not exist.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be thrown if the given version does not match the node's version.
	 * <p>
	 * A KeeperException with error code KeeperException.NotEmpty will be thrown if the node has children.
	 * <p>
	 * This operation, if successful, will trigger all the watches on the node of the given path left by exists API calls, and the watches
	 * on the parent node left by getChildren API calls.
	 * 
	 * @param path
	 *            the path of the node to be deleted.
	 * @param version
	 *            the expected node version.
	 * @throws InterruptedException
	 *             IF the server transaction is interrupted
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero return code.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public void delete(final String path) throws InterruptedException, KeeperException {
		zk.delete(path, -1);
	}

	public void delete(final String path, int version) throws InterruptedException, KeeperException {
		zk.delete(path, version);
	}

	/**
	 * Executes multiple ZooKeeper operations or none of them.
	 * <p>
	 * On success, a list of results is returned. On failure, an exception is raised which contains partial results and error details, see
	 * {@link KeeperException#getResults}
	 * <p>
	 * Note: The maximum allowable size of all of the data arrays in all of the setData operations in this single request is typically 1 MB
	 * (1,048,576 bytes). This limit is specified on the server via <a href=
	 * "http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#Unsafe+Options" >jute.maxbuffer</a>. Requests larger than this will
	 * cause a KeeperException to be thrown.
	 * 
	 * @param ops
	 *            An iterable that contains the operations to be done. These should be created using the factory methods on {@link Op}.
	 * @return A list of results, one for each input Op, the order of which exactly matches the order of the <code>ops</code> input
	 *         operations.
	 * @throws InterruptedException
	 *             If the operation was interrupted. The operation may or may not have succeeded, but will not have partially succeeded if
	 *             this exception is thrown.
	 * @throws KeeperException
	 *             If the operation could not be completed due to some error in doing one of the specified ops.
	 * @since 3.4.0
	 */
	public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
		return zk.multi(ops);
	}

	/**
	 * A Transaction is a thin wrapper on the {@link #multi} method which provides a builder object that can be used to construct and commit
	 * an atomic set of operations.
	 * 
	 * @since 3.4.0
	 * @return a Transaction builder object
	 */
	public Transaction transaction() {
		return zk.transaction();
	}

	/**
	 * The asynchronous version of delete.
	 * 
	 * @see #delete(String, int)
	 */
	public void delete(final String path, int version, VoidCallback cb, Object ctx) {
		zk.delete(path, version, cb, ctx);
	}

	/**
	 * Return the stat of the node of the given path. Return null if no such a node exists.
	 * <p>
	 * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node with the given path.
	 * The watch will be triggered by a successful operation that creates/delete the node or sets the data on the node.
	 * 
	 * @param path
	 *            the node path
	 * @param watcher
	 *            explicit watcher
	 * @return the stat of the node of the given path; return null if no such a node exists.
	 * @throws KeeperException
	 *             If the server signals an error
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */

	public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
		return zk.exists(path, watcher);
	}

	/**
	 * Return the stat of the node of the given path. Return null if no such a node exists.
	 * <p>
	 * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with the given path. The
	 * watch will be triggered by a successful operation that creates/delete the node or sets the data on the node.
	 * 
	 * @param path
	 *            the node path
	 * @param watch
	 *            whether need to watch this node
	 * @return the stat of the node of the given path; return null if no such a node exists.
	 * @throws KeeperException
	 *             If the server signals an error
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 */
	public Stat exists(final String path) throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
		return zk.exists(path, watch);
	}

	/**
	 * The asynchronous version of exists.
	 * 
	 * @see #exists(String, Watcher)
	 */
	public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
		zk.exists(path, watcher, cb, ctx);
	}

	/**
	 * The asynchronous version of exists.
	 * 
	 * @see #exists(String, boolean)
	 */
	public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
		zk.exists(path, watch, cb, ctx);
	}

	/**
	 * Return the data and the stat of the node of the given path.
	 * <p>
	 * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node with the given path.
	 * The watch will be triggered by a successful operation that sets data on the node, or deletes the node.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @param path
	 *            the given path
	 * @param watcher
	 *            explicit watcher
	 * @param stat
	 *            the stat of the node
	 * @return the data of the node
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public byte[] getData(final String path) throws KeeperException, InterruptedException {
		byte[] res = zk.getData(path, false, null);
		return res == null ? new byte[0] : res;
	}

	public String getString(final String path) throws KeeperException, InterruptedException {
		return new String(getData(path));
	}

	public byte[] getData(final String path, boolean watch) throws KeeperException, InterruptedException {
		byte[] res = zk.getData(path, watch, null);
		return res == null ? new byte[0] : res;
	}

	public String getString(final String path, boolean watch) throws KeeperException, InterruptedException {
		return new String(getData(path, watch));
	}

	public byte[] getData(final String path, Watcher watcher) throws KeeperException, InterruptedException {
		byte[] res = zk.getData(path, watcher, null);
		return res == null ? new byte[0] : res;
	}

	public String getString(final String path, Watcher watcher) throws KeeperException, InterruptedException {
		return new String(getData(path, watcher));
	}

	public byte[] getData(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
		byte[] res = zk.getData(path, watcher, stat);
		return res == null ? new byte[0] : res;
	}

	public String getString(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
		return new String(getData(path, watcher, stat));
	}

	/**
	 * Return the data and the stat of the node of the given path.
	 * <p>
	 * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with the given path. The
	 * watch will be triggered by a successful operation that sets data on the node, or deletes the node.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @param path
	 *            the given path
	 * @param watch
	 *            whether need to watch this node
	 * @param stat
	 *            the stat of the node
	 * @return the data of the node
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 */
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
		byte[] res = zk.getData(path, watch, stat);
		return res == null ? new byte[0] : res;
	}

	public String getString(final String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
		return new String(getData(path, watch, stat));
	}

	/**
	 * The asynchronous version of getData.
	 * 
	 * @see #getData(String, Watcher, Stat)
	 */
	public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
		zk.getData(path, watcher, cb, ctx);
	}

	/**
	 * The asynchronous version of getData.
	 * 
	 * @see #getData(String, boolean, Stat)
	 */
	public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
		zk.getData(path, watch, cb, ctx);
	}

	/**
	 * Set the data for the node of the given path if such a node exists and the given version matches the version of the node (if the given
	 * version is -1, it matches any node's versions). Return the stat of the node.
	 * <p>
	 * This operation, if successful, will trigger all the watches on the node of the given path left by getData calls.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be thrown if the given version does not match the node's version.
	 * <p>
	 * The maximum allowable size of the data array is 1 MB (1,048,576 bytes). Arrays larger than this will cause a KeeperException to be
	 * thrown.
	 * 
	 * @param path
	 *            the path of the node
	 * @param data
	 *            the data to set
	 * @param version
	 *            the expected matching version
	 * @return the state of the node
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public Stat setData(final String path, byte data[], int version) throws KeeperException, InterruptedException {
		return zk.setData(path, data, version);
	}

	public Stat setData(final String path, byte data[]) throws KeeperException, InterruptedException {
		return zk.setData(path, data, -1);
	}

	public Stat setData(final String path, String data) throws KeeperException, InterruptedException {
		return zk.setData(path, data.getBytes(), -1);
	}

	/**
	 * The asynchronous version of setData.
	 * 
	 * @see #setData(String, byte[], int)
	 */
	public void setData(final String path, byte data[], int version, StatCallback cb, Object ctx) {
		zk.setData(path, data, version, cb, ctx);
	}

	/**
	 * Return the ACL and stat of the node of the given path.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @param path
	 *            the given path for the node
	 * @param stat
	 *            the stat of the node will be copied to this parameter.
	 * @return the ACL array of the given node.
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public List<ACL> getACL(final String path, Stat stat) throws KeeperException, InterruptedException {
		return zk.getACL(path, stat);
	}

	/**
	 * The asynchronous version of getACL.
	 * 
	 * @see #getACL(String, Stat)
	 */
	public void getACL(final String path, Stat stat, ACLCallback cb, Object ctx) {
		zk.getACL(path, stat, cb, ctx);
	}

	/**
	 * Set the ACL for the node of the given path if such a node exists and the given version matches the version of the node. Return the
	 * stat of the node.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be thrown if the given version does not match the node's version.
	 * 
	 * @param path
	 * @param acl
	 * @param version
	 * @return the stat of the node.
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 * @throws org.apache.zookeeper.KeeperException.InvalidACLException
	 *             If the acl is invalide.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public Stat setACL(final String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
		return zk.setACL(path, acl, version);
	}

	/**
	 * The asynchronous version of setACL.
	 * 
	 * @see #setACL(String, List, int)
	 */
	public void setACL(final String path, List<ACL> acl, int version, StatCallback cb, Object ctx) {
		zk.setACL(path, acl, version, cb, ctx);

	}

	/**
	 * Return the list of the children of the node of the given path.
	 * <p>
	 * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node with the given path.
	 * The watch willbe triggered by a successful operation that deletes the node of the given path or creates/delete a child under the
	 * node.
	 * <p>
	 * The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @param path
	 * @param watcher
	 *            explicit watcher
	 * @return an unordered array of children of the node with the given path
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException {
		return zk.getChildren(path, watcher);
	}

	/**
	 * Return the list of the children of the node of the given path.
	 * <p>
	 * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with the given path. The
	 * watch willbe triggered by a successful operation that deletes the node of the given path or creates/delete a child under the node.
	 * <p>
	 * The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @param path
	 * @param watch
	 * @return an unordered array of children of the node with the given path
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 */
	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
		return zk.getChildren(path, watch);
	}

	/**
	 * The asynchronous version of getChildren.
	 * 
	 * @see #getChildren(String, Watcher)
	 */
	public void getChildren(final String path, Watcher watcher, ChildrenCallback cb, Object ctx) {
		zk.getChildren(path, watcher, cb, ctx);
	}

	/**
	 * The asynchronous version of getChildren.
	 * 
	 * @see #getChildren(String, boolean)
	 */
	public void getChildren(String path, boolean watch, ChildrenCallback cb, Object ctx) {
		zk.getChildren(path, watch, cb, ctx);
	}

	/**
	 * For the given znode path return the stat and children list.
	 * <p>
	 * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node with the given path.
	 * The watch willbe triggered by a successful operation that deletes the node of the given path or creates/delete a child under the
	 * node.
	 * <p>
	 * The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @since 3.3.0
	 * @param path
	 * @param watcher
	 *            explicit watcher
	 * @param stat
	 *            stat of the znode designated by path
	 * @return an unordered array of children of the node with the given path
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public List<String> getChildren(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
		return zk.getChildren(path, watcher, stat);
	}

	/**
	 * For the given znode path return the stat and children list.
	 * <p>
	 * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with the given path. The
	 * watch willbe triggered by a successful operation that deletes the node of the given path or creates/delete a child under the node.
	 * <p>
	 * The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
	 * 
	 * @since 3.3.0
	 * @param path
	 * @param watch
	 * @param stat
	 *            stat of the znode designated by path
	 * @return an unordered array of children of the node with the given path
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 */
	public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
		return zk.getChildren(path, watch, stat);
	}

	/**
	 * The asynchronous version of getChildren.
	 * 
	 * @since 3.3.0
	 * @see #getChildren(String, Watcher, Stat)
	 */
	public void getChildren(final String path, Watcher watcher, Children2Callback cb, Object ctx) {
		zk.getChildren(path, watcher, cb, ctx);
	}

	/**
	 * The asynchronous version of getChildren.
	 * 
	 * @since 3.3.0
	 * @see #getChildren(String, boolean, Stat)
	 */
	public void getChildren(String path, boolean watch, Children2Callback cb, Object ctx) {
		zk.getChildren(path, watch, cb, ctx);
	}

	/**
	 * Asynchronous sync. Flushes channel between process and leader.
	 * 
	 * @param path
	 * @param cb
	 *            a handler for the callback
	 * @param ctx
	 *            context to be provided to the callback
	 * @throws IllegalArgumentException
	 *             if an invalid path is specified
	 */
	public void sync(final String path, VoidCallback cb, Object ctx) {
		zk.sync(path, cb, ctx);
	}

	public States getState() {
		return zk.getState();
	}

	/**
	 * String representation of this ZooKeeper client. Suitable for things like logging. Do NOT count on the format of this string, it may
	 * change without warning.
	 * 
	 * @since 3.3.0
	 */
	@Override
	public String toString() {
		return zk.toString();
	}

	public void createPaths(String path, String data) throws IOException, KeeperException, InterruptedException {
		String[] paths = path.split("/");
		path = "";
		for (int i = 1; i < paths.length; i++) {
			path += "/" + paths[i];
			if (exists(path) == null) {
				// System.err.println("createPaths:" + path);
				this.createPresistentNode(path, null);
			}
		}
		this.setData(path, data);
	}

	public void deleteRecursive(final String pathRoot) throws InterruptedException, KeeperException {
		if (zk.exists(pathRoot, false) != null)
			ZKUtil.deleteRecursive(zk, pathRoot);
	}

	public static void main(String[] args) throws IOException {
		System.out.println(ZooKeeperServer.class.toString());
		ZooKeeperServer server = new ZooKeeperServer("localhost:2181");
		ZooKeeperServer server2 = new ZooKeeperServer("localhost:2181,localhost:2182,localhost:2183");

		MultiWatcher watch = server.getWatch();
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
		}
	}
}