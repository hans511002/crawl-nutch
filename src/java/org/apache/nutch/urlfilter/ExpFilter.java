package org.apache.nutch.urlfilter;

public abstract class ExpFilter implements java.io.Serializable {
	private static final long serialVersionUID = 2192639428976171640L;

	public abstract boolean filter(String url);

	public abstract String toString();

	public abstract boolean equals(ExpFilter e);
}
