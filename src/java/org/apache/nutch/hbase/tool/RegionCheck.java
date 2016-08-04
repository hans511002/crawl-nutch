package org.apache.nutch.hbase.tool;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionCheck {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
			HTableDescriptor[] tables = admin.listTables();
			DateFormat formatter = DateFormat.getDateTimeInstance();
			for (HTableDescriptor tab : tables) {
				String tableName = tab.getNameAsString();
				HTable table = new HTable(HBaseConfiguration.create(), tableName);
				ClusterStatus cs = admin.getClusterStatus();
				Map<HRegionInfo, ServerName> regions = table.getRegionLocations();
				if (regions != null && regions.size() > 0) {
					for (Map.Entry<HRegionInfo, ServerName> hriEntry : regions.entrySet()) {
						HRegionInfo regionInfo = hriEntry.getKey();
						ServerLoad load = cs.getLoad(ServerName.parseServerName(hriEntry.getValue().getServerName()));
						if (load != null && load.getRegionsLoad().get(regionInfo.getRegionName()) != null) {
							System.out.println(formatter.format(new Date()) + " " + Bytes.toStringBinary(regionInfo.getRegionName()) + " "
									+ hriEntry.getValue().getServerName());
						} else {
							System.err.println(formatter.format(new Date()) + " " + Bytes.toStringBinary(regionInfo.getRegionName())
									+ " is Region Hole, and try to reassign again.");
							admin.assign(regionInfo.getRegionName());
						}
					}
				}
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
