package org.apache.gora.mongo.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nutch.util.Bytes;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

public class MongoUtil {
	public final MongoCollection<Document> docCell;
	private final MongoClient client;

	public MongoUtil(String host, int port, String dbName, String tableName) {
		client = new MongoClient(host, port);
		docCell = client.getDatabase(dbName).getCollection(tableName);
	}

	public List<Map> get(BasicDBObject qukey) {
		MongoCursor<Document> dcus = null;
		try {
			FindIterable<Document> result = docCell.find(qukey);
			List<Map> res = null;
			if (result != null) {
				result.batchSize(100);
				dcus = result.iterator();
				res = new ArrayList<Map>();
				while (dcus.hasNext()) {
					org.bson.Document doc = dcus.next();
					Map<String, Object> _res = new HashMap<String, Object>();
					for (String key : doc.keySet()) {
						_res.put(key, doc.get(key));
					}
					res.add(_res);
				}
				dcus.close();
			}
			return res;
		} finally {
			if (dcus != null)
				dcus.close();
		}
	}

	public UpdateResult put(BasicDBObject term, BasicDBObject doc) {
		synchronized (docCell) {
			UpdateOptions up = new UpdateOptions();
			up.upsert(true);
			return this.docCell.updateOne(term, new BasicDBObject("$set", doc), up);
		}
	}

	public UpdateResult put(Map<String, Object> _term, Map<String, Object> _doc) {
		BasicDBObject term = new BasicDBObject();
		BasicDBObject doc = new BasicDBObject();
		for (String key : _term.keySet()) {
			putSubMap(term, key, _term.get(key));
		}
		for (String key : _doc.keySet()) {
			putSubMap(doc, key, _doc.get(key));
		}
		synchronized (docCell) {
			UpdateOptions up = new UpdateOptions();
			up.upsert(true);
			return this.docCell.updateOne(term, new BasicDBObject("$set", doc), up);
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
}
