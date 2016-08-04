package org.apache.nutch.hbase.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class RegionServerCheck {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			ClusterStatus cs = admin.getClusterStatus();

			for (ServerName ser : cs.getDeadServerNames()) {
				System.out.println("DeadServerName :" + ser);
			}
			for (ServerName ser : cs.getServers()) {
				System.out.println("ServerName :" + ser);
			}
			for (ServerName ser : cs.getBackupMasters()) {
				System.out.println("BackupMasters :" + ser);
			}
			if (cs.getDeadServerNames().size() > 0) {
				Process proc = Runtime.getRuntime().exec("start-hbase.sh");
				BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
				String text = null;
				while ((text = in.readLine()) != null) {
					if (text == null || text.trim().equals("")) {
						continue;
					}
					System.out.println(text);
				}
				in.close();
				System.out.println("=====================================================");
				for (ServerName ser : cs.getServers()) {
					System.out.println("ServerName :" + ser);
				}
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
