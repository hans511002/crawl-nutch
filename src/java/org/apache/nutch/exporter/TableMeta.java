package org.apache.nutch.exporter;

import java.util.List;

public class TableMeta {
	private String topicId;
	private String name;
	private List<String> cols;
	
	public TableMeta(){}
	
	public TableMeta(String mame, List<String> cols) {
		this.name = mame;
		this.cols = cols;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getTopicId() {
		return topicId;
	}
	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}
	public List<String> getCols() {
		return cols;
	}
	public void setCols(List<String> cols) {
		this.cols = cols;
	}
}
