package org.apache.gora.mongo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.gora.mongo.query.MongoCurResult;
import org.apache.gora.mongo.query.MongoQuery;
import org.apache.gora.mongo.util.MongoObjectInterface;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.StatefulMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.Bytes;
import org.bson.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.sobey.jcg.support.log4j.LogUtils;
import com.sobey.jcg.support.utils.Convert;

/**
 * DataStore for HBase. Thread safe.
 * 
 */
public class MongoStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {

	public static final Logger LOG = LoggerFactory.getLogger(MongoStore.class);

	public static final String PARSE_MAPPING_FILE_KEY = "gora.mongo.mapping.file";

	public static final String DEFAULT_MAPPING_FILE = "gora-mongo-mapping.xml";
	private volatile MongoClient client;
	private volatile DB db;
	private volatile DBCollection table;
	private volatile MongoCollection<Document> docCell;

	private final boolean autoCreateSchema = true;

	public MongoMapping mapping;

	public static final String mongoDBURL = "mongodb.url";
	public static final String mongoDBPort = "mongodb.port";
	public static final String mongoDBName = "mongodb.dbname";

	String dbName;

	public MongoStore() {
	}

	@Override
	public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
		try {
			super.initialize(keyClass, persistentClass, properties);
			client = new MongoClient(properties.getProperty(mongoDBURL, "localhost"), Convert.toInt(properties
					.getProperty(mongoDBPort, "27017")));
			dbName = properties.getProperty(mongoDBName, "testdb");
			db = client.getDB(dbName);
			mapping = readMapping(properties.getProperty(PARSE_MAPPING_FILE_KEY, DEFAULT_MAPPING_FILE));
			if (autoCreateSchema) {
				createSchema();
			}
			LogUtils.info(mapping.toString());
			table = db.getCollection(mapping.tableName);
			docCell = client.getDatabase(dbName).getCollection(mapping.tableName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getSchemaName() {
		return mapping.getTableName();
	}

	@Override
	public void createSchema() {
		if (schemaExists()) {
			return;
		}
		db.createCollection(mapping.tableName, null);
	}

	@Override
	public void deleteSchema() {
		table.drop();
	}

	@Override
	public boolean schemaExists() {
		return db.getCollection(mapping.getTableName()) != null;
	}

	@Override
	public T get(K key, String[] fields) {
		try {
			fields = getFieldsToQuery(fields);
			BasicDBObject get = new BasicDBObject();
			get.put(this.mapping.primaryColumn, key);
			FindIterable<Document> result = docCell.find(get);
			if (result != null) {
				return newInstance(result.first(), fields);
			}
			return null;
		} catch (IOException ex2) {
			LOG.error(ex2.getMessage());
			LOG.error(ex2.getStackTrace().toString());
			return null;
		}
	}

	public void putRecord(BasicDBObject doc, String hcol, Object o) {
		BasicDBObject rc = new BasicDBObject();
		if (o instanceof PersistentBase) {
			PersistentBase ps = (PersistentBase) o;
			for (String fieldName : ps.getFields()) {
				Field f = ps.getSchema().getField(fieldName);
				Type type = f.schema().getType();
				o = ps.get(ps.getFieldIndex(fieldName));
				if (o instanceof Utf8) {
					o = o.toString();
				}
				switch (type) {
				case RECORD:
					putRecord(rc, fieldName, o);
					doc.put(hcol, rc);
					break;
				case MAP:
					if (o instanceof StatefulMap) {
						StatefulHashMap<Utf8, ?> map = (StatefulHashMap<Utf8, ?>) o;
						BasicDBObject smap = new BasicDBObject();
						for (Entry<Utf8, State> e : map.states().entrySet()) {
							Utf8 mapKey = e.getKey();
							switch (e.getValue()) {
							case DELETED:
								break;
							default:
								String qual = mapKey.toString();
								Object val = map.get(mapKey);
								smap.put(qual, val.toString());
								break;
							}
						}
						putSubMap(doc, fieldName, smap);
					} else {
						Set<Map.Entry> set = ((Map) o).entrySet();
						BasicDBObject smap = new BasicDBObject();
						for (Entry entry : set) {
							String qual = entry.getKey().toString();
							Object val = entry.getValue();
							smap.put(qual, val);
						}
						putSubMap(doc, fieldName, smap);
					}
					break;
				case ARRAY:
					if (o instanceof GenericArray) {
						ArrayList<Object> smap = new ArrayList<Object>();
						GenericArray arr = (GenericArray) o;
						int j = 0;
						for (Object item : arr) {
							smap.add(item);
						}
						putSubMap(doc, fieldName, smap);
					}
					break;
				default:
					putSubMap(doc, fieldName, o);
					break;
				}
			}
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void put(K key, T persistent) {
		Schema schema = persistent.getSchema();
		StateManager stateManager = persistent.getStateManager();
		BasicDBObject doc = new BasicDBObject();
		doc.put(this.mapping.primaryColumn, key.toString());
		Iterator<Field> iter = schema.getFields().iterator();
		for (int i = 0; iter.hasNext(); i++) {
			Field field = iter.next();
			if (!stateManager.isDirty(persistent, i)) {
				continue;
			}
			Type type = field.schema().getType();
			Object o = persistent.get(i);
			if (o == null)
				continue;
			if (o instanceof Utf8) {
				o = o.toString();
			}
			MongoColumn hcol = mapping.getColumn(field.name());
			if (hcol == null) {
				throw new RuntimeException("Mongo mapping for field [" + persistent.getClass().getName() + "#"
						+ field.name() + "] not found. Wrong gora-mongo-mapping.xml?");
			}
			if (hcol.getName().equals("outlinks") || hcol.getName().equals("inlinks")) {
				continue;
			}
			switch (type) {
			case RECORD:
				putRecord(doc, hcol.getName(), o);
				break;
			case MAP:
				if (o instanceof StatefulMap) {
					StatefulHashMap<Utf8, ?> map = (StatefulHashMap<Utf8, ?>) o;
					BasicDBObject smap = new BasicDBObject();
					for (Entry<Utf8, State> e : map.states().entrySet()) {
						Utf8 mapKey = e.getKey();
						switch (e.getValue()) {
						case DELETED:
							break;
						default:
							String qual = mapKey.toString();
							Object val = map.get(mapKey);
							smap.put(qual, val.toString());
							break;
						// case DIRTY:
						// String qual = mapKey.toString();
						// Object val = map.get(mapKey);
						// smap.put(qual, val.toString());
						// break;
						// case DELETED:
						// qual = mapKey.toString();
						// smap.put(qual, null);
						// break;
						}
					}
					putSubMap(doc, hcol.getName(), smap);
				} else {
					Set<Map.Entry> set = ((Map) o).entrySet();
					BasicDBObject smap = new BasicDBObject();
					for (Entry entry : set) {
						String qual = entry.getKey().toString();
						Object val = entry.getValue();
						if (val instanceof Utf8) {
							val = o.toString();
						}
						smap.put(qual, val);
					}
					putSubMap(doc, hcol.getName(), smap);
				}
				break;
			case ARRAY:
				if (o instanceof GenericArray) {
					ArrayList<Object> smap = new ArrayList<Object>();
					GenericArray arr = (GenericArray) o;
					int j = 0;
					for (Object item : arr) {
						if (item instanceof Utf8) {
							item = o.toString();
						}
						smap.add(item);
					}
					putSubMap(doc, hcol.getName(), smap);
				}
				break;
			default:
				String colNamePath = hcol.getName();
				putSubMap(doc, colNamePath, o);
				// put.add(hcol.getFamily() ,to(o, field.schema()));
				break;
			}
		}
		// LogUtils.info(doc.toJson());
		// db.getCollection(this.mapping.tableName)
		synchronized (table) {
			BasicDBObject bl = new BasicDBObject();
			bl.put(this.mapping.primaryColumn, key.toString());
			UpdateOptions up = new UpdateOptions();
			up.upsert(true);
			UpdateResult res = this.docCell.updateOne(bl, new BasicDBObject("$set", doc), up);
			// WriteResult res = table.update(bl, new BasicDBObject("$set", doc), true, false);
		}
	}

	void putSubMap(BasicDBObject col, String colNamePath, Object o) {
		String colNames[] = colNamePath.split("\\.");
		Object val = o;
		if (o instanceof java.nio.ByteBuffer) {
			val = Bytes.toString(((ByteBuffer) o).array());
		}
		for (int j = 0; j < colNames.length - 1; j++) {
			BasicDBObject _col = (BasicDBObject) col.get(colNames[j]);
			if (_col == null) {
				_col = new BasicDBObject();
				col.put(colNames[j], _col);
			}
			col = _col;
		}
		col.put(colNames[colNames.length - 1], val);
	}

	/**
	 * Deletes the object with the given key.
	 * 
	 * @return always true
	 */
	@Override
	public boolean delete(K key) {
		WriteResult res = table.remove(new BasicDBObject(this.mapping.primaryColumn, key));
		return res.getN() > 0;
	}

	@Override
	public long deleteByQuery(Query<K, T> query) {
		try {
			org.apache.gora.query.Result<K, T> result = null;
			result = query.execute();
			long delSize = 0;
			while (result.next()) {
				if (delete(result.getKey())) {
					delSize++;
				}
			}
			return delSize;
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.error(ex.getStackTrace().toString());
			return -1;
		}
	}

	@Override
	public void flush() {
	}

	@Override
	public Query<K, T> newQuery() {
		return new MongoQuery<K, T>(this);
	}

	@Override
	public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query) throws IOException {
		// Pair<byte[][], byte[][]> keys = null;
		// if (query.getStartKey() != HConstants.EMPTY_START_ROW || query.getEndKey() != HConstants.EMPTY_END_ROW) {
		// byte[] startKey = HConstants.EMPTY_START_ROW;
		// if (query.getStartKey() != null) {
		// startKey = toBytes(query.getStartKey());
		// }
		// byte[] endKey = HConstants.EMPTY_END_ROW;
		// if (query.getEndKey() != null) {
		// endKey = toBytes(query.getEndKey());
		// }
		// keys = table.getInRangeEndKeys(startKey, endKey);
		// } else {
		// keys = table.getStartEndKeys();
		// }
		// if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
		// throw new IOException("Expecting at least one region.");
		// }
		// if (table == null) {
		// throw new IOException("No table was provided.");
		// }
		List<PartitionQuery<K, T>> partitions = new ArrayList<PartitionQuery<K, T>>();
		// byte[] startRow = query.getStartKey() != null ? toBytes(query.getStartKey()) : HConstants.EMPTY_START_ROW;
		// byte[] stopRow = query.getEndKey() != null ? toBytes(query.getEndKey()) : HConstants.EMPTY_END_ROW;
		// for (int i = keys.getFirst().length - 1; i >= 0; i--) {
		// String regionLocation = table.getRegionLocation(keys.getFirst()[i]).getHostname();
		//
		// // determine if the given start an stop key fall into the region
		// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow,
		// keys.getSecond()[i]) < 0)
		// && (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
		//
		// byte[] splitStart = startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
		// .getFirst()[i] : startRow;
		//
		// byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0)
		// && keys.getSecond()[i].length > 0 ? keys.getSecond()[i] : stopRow;
		//
		// K startKey = Arrays.equals(HConstants.EMPTY_START_ROW, splitStart) ? null : MongoByteInterface
		// .fromBytes(keyClass, splitStart);
		// K endKey = Arrays.equals(HConstants.EMPTY_END_ROW, splitStop) ? null : MongoByteInterface.fromBytes(
		// keyClass, splitStop);
		//
		// PartitionQuery<K, T> partition = new PartitionQueryImpl<K, T>(query, startKey, endKey, regionLocation);
		PartitionQuery<K, T> partition = new PartitionQueryImpl<K, T>(query, "", "", null);
		partitions.add(partition);
		// }
		// }
		return partitions;
	}

	BasicDBObject othTerm = new BasicDBObject();

	public void setOthterm(BasicDBObject othTerm) {
		this.othTerm = othTerm;
	}

	@Override
	public org.apache.gora.query.Result<K, T> execute(Query<K, T> query) {
		query.setFields(getFieldsToQuery(query.getFields()));
		MongoCursor<Document> result = null;
		BasicDBObject map = new BasicDBObject();
		// { "baseUrl": { $gte: "https://www.marklines.com/cn/global/3609" }, $and: [ { "baseUrl": { $lte:
		// "https://www.marklines.com/cn/global/3609" } } ] }
		BasicDBObject term = new BasicDBObject();
		if (query.getStartKey() != null && !query.getStartKey().equals("")) {
			// { "baseUrl": { $gte: "https://www.marklines.com/cn/supplier_db/partDetail/top?_is=trud", $lte:
			// "https://www.marklines.com/cn/supplier_db/partDetail/top?_is=truf" } }
			term.put("$gte", query.getStartKey());
		}
		if (query.getEndKey() != null && !query.getEndKey().equals("")) {
			term.put("$lte", query.getEndKey());
		}
		if (term.size() > 0) {
			map.put(this.mapping.primaryColumn, term);
		}
		for (String entry : othTerm.keySet()) {
			map.put(entry, othTerm.get(entry));
		}
		if (map.size() > 0) {
			result = docCell.find(map).iterator();
		} else {
			result = docCell.find().iterator();
		}
		return new MongoCurResult<K, T>(this, query, result);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T newInstance(org.bson.Document result, String[] fields) throws IOException {
		if (result == null || result.isEmpty())
			return null;
		T persistent = newPersistent();
		StateManager stateManager = persistent.getStateManager();
		for (String f : fields) {
			MongoColumn col = mapping.getColumn(f);
			if (col == null) {
				throw new RuntimeException("HBase mapping for field [" + f + "] not found. "
						+ "Wrong gora-hbase-mapping.xml?");
			}
			Field field = fieldMap.get(f);
			Schema fieldSchema = field.schema();
			switch (fieldSchema.getType()) {
			case MAP:
				org.bson.Document o = (org.bson.Document) result.get(col.getName());
				if (o == null) {
					continue;
				}
				Map map = new HashMap();
				for (String key : o.keySet()) {
					map.put(key, o.get(key));
				}
				setField(persistent, field, map);
				break;
			case ARRAY:
				ArrayList<Object> list = (ArrayList<Object>) result.get(col.getName());
				if (list == null) {
					continue;
				}
				ListGenericArray arr = new ListGenericArray(fieldSchema, list);
				setField(persistent, field, arr);
				break;
			default:
				Object val = result.get(col.getName());
				// byte[] val = result.getValue(col.getFamily(), col.getQualifier());
				if (val == null) {
					continue;
				}
				setField(persistent, field, val);
				break;
			}
		}
		stateManager.clearDirty(persistent);
		return persistent;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setField(T persistent, Field field, Map map) {
		persistent.put(field.pos(), new StatefulHashMap(map));
	}

	private void setField(T persistent, Field field, Object val) throws IOException {
		persistent.put(field.pos(), MongoObjectInterface.fromObject(field.schema(), val));
	}

	@SuppressWarnings("rawtypes")
	private void setField(T persistent, Field field, GenericArray list) {
		persistent.put(field.pos(), list);
	}

	@SuppressWarnings("unchecked")
	private MongoMapping readMapping(String filename) throws IOException {
		try {
			MongoMapping mongoMap = new MongoMapping();
			SAXBuilder builder = new SAXBuilder();
			org.jdom.Document doc = builder.build(getClass().getClassLoader().getResourceAsStream(filename));
			Element root = doc.getRootElement();
			List<Element> classElements = root.getChildren("class");
			for (Element classElement : classElements) {
				if (classElement.getAttributeValue("keyClass").equals(keyClass.getCanonicalName())
						&& classElement.getAttributeValue("name").equals(persistentClass.getCanonicalName())) {

					String tableNameFromMapping = classElement.getAttributeValue("table");
					String tableName = getSchemaName(tableNameFromMapping, persistentClass);

					// tableNameFromMapping could be null here
					if (!tableName.equals(tableNameFromMapping)) {
						LOG.info("Keyclass and nameclass match but mismatching table names "
								+ " mappingfile schema is '" + tableNameFromMapping + "' vs actual schema '"
								+ tableName + "' , assuming they are the same.");
					}
					mongoMap.setTableName(tableName);
					List<Element> pkey = classElement.getChildren("primarykey");
					if (pkey != null && pkey.size() == 1) {
						mongoMap.primaryColumn = pkey.get(0).getAttributeValue("column");
					}
					List<Element> fields = classElement.getChildren("field");
					for (Element field : fields) {
						String fieldName = field.getAttributeValue("name");
						String column = field.getAttributeValue("column");
						String iskey = field.getAttributeValue("primarykey");
						mongoMap.columnMap.put(fieldName, new MongoColumn(column, Convert.toBool(iskey, false)));
					}
					break;
				}
			}
			return mongoMap;
		} catch (IOException ex) {
			LOG.error(ex.getMessage());
			LOG.error(ex.getStackTrace().toString());
			throw ex;
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.error(ex.getStackTrace().toString());
			throw new IOException(ex);
		}

	}

	@Override
	public void close() {
		if (client != null) {
			client.close();
			client = null;
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
