package org.apache.nutch.parse.element;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.ObjectCache;

public class SegMentParsers {

	public static final String SegMentParse_ORDER = "segmentparser.order";
	private SegMentParse[] filters;
	private ConfigAttrParses cfgParse = null;

	public static enum Field {
		BASE_URL("baseUrl"), FETCH_TIME("fetchTime"), MODIFIED_TIME("modifiedTime"), CONTENT("content"), CONTENT_TYPE(
				"contentType"), TITLE("title"), TEXT("text"), HEADERS("headers"), METADATA("metadata"), CONFIGURL(
				"configUrl");

		Field(String name) {
			this.name = name;
		}

		private String name;

		public String getName() {
			return name;
		}

		public static Field getFiled(String filed) {

			if ("content".equals(filed))
				return CONTENT;
			if ("title".equals(filed))
				return TITLE;
			if ("baseUrl".equals(filed))
				return BASE_URL;
			if ("contentType".equals(filed))
				return CONTENT_TYPE;
			if ("fetchTime".equals(filed))
				return FETCH_TIME;
			if ("modifiedTime".equals(filed))
				return MODIFIED_TIME;
			if ("headers".equals(filed))
				return HEADERS;
			if ("metadata".equals(filed))
				return METADATA;
			if ("text".equals(filed))
				return TEXT;
			if ("configUrl".equals(filed))
				return CONFIGURL;
			return null;
		}

	}

	public SegMentParsers(Configuration conf) {
		this(conf, true);
	}

	public SegMentParsers(Configuration conf, boolean idbConfig) {
		if (idbConfig) {
			cfgParse = new ConfigAttrParses();
			cfgParse.setConf(conf);
		}
		String order = conf.get(SegMentParse_ORDER);
		ObjectCache objectCache = ObjectCache.get(conf);
		this.filters = (SegMentParse[]) objectCache.getObject(SegMentParse.class.getName());
		if (this.filters == null) {
			String[] orderedFilters = null;
			if (order != null && !order.trim().equals("")) {
				orderedFilters = order.split("\\s+");
			}
			try {
				ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(SegMentParse.X_POINT_ID);
				if (point == null)
					throw new RuntimeException(SegMentParse.X_POINT_ID + " not found.");
				Extension[] extensions = point.getExtensions();
				Map<String, SegMentParse> filterMap = new HashMap<String, SegMentParse>();
				for (int i = 0; i < extensions.length; i++) {
					Extension extension = extensions[i];
					SegMentParse filter = (SegMentParse) extension.getExtensionInstance();
					if (!filterMap.containsKey(filter.getClass().getName())) {
						filterMap.put(filter.getClass().getName(), filter);
					}
				}
				ArrayList<SegMentParse> filters = new ArrayList<SegMentParse>();
				if (orderedFilters != null) {
					for (int i = 0; i < orderedFilters.length; i++) {
						SegMentParse filter = filterMap.get(orderedFilters[i]);
						if (filter != null) {
							filters.add(filter);
						}
					}
				}
				for (String name : filterMap.keySet()) {
					SegMentParse filter = filterMap.get(name);
					if (!filters.contains(filter)) {
						filters.add(filter);
					}
				}
				objectCache.setObject(SegMentParse.class.getName(), filters.toArray(new SegMentParse[filters.size()]));
			} catch (PluginRuntimeException e) {
				throw new RuntimeException(e);
			}
			this.filters = (SegMentParse[]) objectCache.getObject(SegMentParse.class.getName());
		}
	}

	public boolean parser(String unreverseKey, WebPage page, WebPageSegment psg) throws Exception {
		boolean res = false;
		if (cfgParse != null) {
			res = cfgParse.parseSegMent(unreverseKey, page, psg);
			if (!res)
				return false;
		}
		res = false;
		for (int i = 0; i < this.filters.length; i++) {
			boolean r = this.filters[i].parseSegMent(unreverseKey, page, psg);
			if (res == false)
				res = r;
		}
		return res;
	}

	public Collection<WebPage.Field> getFields(Job job) {
		Collection<WebPage.Field> fields = new HashSet<WebPage.Field>();
		for (SegMentParse parse : filters) {
			Collection<WebPage.Field> pluginFields = parse.getFields();
			if (pluginFields != null) {
				fields.addAll(pluginFields);
			}
		}
		return fields;
	}
}
