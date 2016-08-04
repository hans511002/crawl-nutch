package org.apache.nutch.exporter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.indexer.NutchDocument;

import com.sobey.jcg.support.jdbc.DataAccess;
import com.sobey.jcg.support.jdbc.DataSourceImpl;

public class DbExporter {

	private Configuration conf;
	private Connection conn = null;
	private DataAccess access = null;
	private Map<String, TableMeta> tableMapping = new HashMap<String, TableMeta>();
	private TableMeta tableMeta = null;
	private boolean columnAuto = false;
	private String columnSql = null;

	public void open(TaskAttemptContext job) throws Exception {
		open(job.getConfiguration());
	}

	public void open(Configuration conf) throws Exception {
		this.conf = conf;
		com.sobey.jcg.support.jdbc.DataSourceImpl dataSource = new DataSourceImpl(
				conf.get(NutchConstant.exporterDbDriverName), conf.get(NutchConstant.exporterDbUrl),
				conf.get(NutchConstant.exporterDbUser), conf.get(NutchConstant.exporterDbPass));
		conn = dataSource.getConnection();
		access = new DataAccess(conn);

		columnAuto = conf.getBoolean("export.db.column.auto", false);
		columnSql = conf.get("export.db.column.sql");
		initTableMeta();
	}

	public void close() {
		if (null != conn) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public boolean write(long typid, NutchDocument doc) {
		try {
			TableMeta tm = getTableMeta(typid + "");
			if (doc != null && null != tm) {
				Collection<String> fields = doc.getFieldNames();
				if (columnAuto) {
					alterTable(fields, tm);
				}
				StringBuffer sql = new StringBuffer("INSERT INTO " + tm.getName() + " (");
				List<Object> params = new ArrayList<Object>();
				List<String> cols = tm.getCols();
				for (String field : fields) {
					for (String col : cols) {
						if (field.toUpperCase().equals(col.toUpperCase())) {
							sql.append(col + ",");
							params.add(doc.getFieldValue(field));
							break;
						}
					}
				}
				sql.deleteCharAt(sql.length() - 1);
				sql.append(") VALUES(");
				for (int i = 0; i < params.size(); i++) {
					sql.append("?,");
				}
				sql.deleteCharAt(sql.length() - 1);
				sql.append(")");
				access.execUpdate(sql.toString(), params.toArray());
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	// private boolean exists(String rowkey, String table){
	// String sql = "SELECT COUNT(*) FROM " + table + " WHERE ROWKEY=?";
	// long total = access.queryForLong(sql, rowkey);
	// return total>0 ? true:false;
	// }

	private TableMeta getTableMeta(String topicId) {
		if (tableMapping.containsKey(topicId)) {
			return tableMapping.get(topicId);
		}
		return tableMeta;
	}

	private void initTableMeta() throws Exception {
		String table = conf.get(NutchConstant.exporterDbTable).trim().toUpperCase();
		if (table.indexOf(":") != -1) {
			String[] arr = table.split(";");
			for (String str : arr) {
				if (str.indexOf(":") != -1) {
					String[] temp = str.split(":");
					String tn = temp[0].trim();
					TableMeta tm = new TableMeta(tn, getCols(tn));
					String topicId = temp[1].trim();
					if (topicId.indexOf(",") != -1) {
						String[] tids = topicId.split(",");
						for (String id : tids) {
							tableMapping.put(id, tm);
						}
					} else {
						tableMapping.put(topicId, tm);
					}
				} else {
					tableMeta = new TableMeta(str, getCols(str));
				}
			}
		} else {
			tableMeta = new TableMeta(table, getCols(table));
		}
	}

	private List<String> getCols(String tableName) throws SQLException {
		if (null != tableName && !"".equals(tableName)) {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet resultSet = metaData.getColumns(null, null, tableName, "%");
			List<String> cols = new ArrayList<String>();
			while (resultSet.next()) {
				cols.add(resultSet.getString("COLUMN_NAME").toUpperCase());
			}
			return cols;
		}
		return null;
	}

	private void alterTable(Collection<String> fields, TableMeta tm) {
		List<String> sqls = new ArrayList<String>();
		List<String> cols = tm.getCols();
		for (String field : fields) {
			boolean existsCol = false;
			for (String col : cols) {
				if (field.toUpperCase().equals(col.toUpperCase())) {
					existsCol = true;
					break;
				}
			}
			if (false == existsCol && field.startsWith("cnt_")) {
				cols.add(field);
				System.out.println(tm.getName() + " : " + field);
				String sql = columnSql.replace("{table}", tm.getName()).replace("{column}", field);
				sqls.add(sql);
			}
		}
		if (sqls.size() > 0) {
			access.execUpdateBatch(sqls.toArray(new String[sqls.size()]));
		}
	}

}
