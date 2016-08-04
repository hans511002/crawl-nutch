package org.apache.nutch.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.URLPartitioner.SelectorEntryPartitioner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorJob extends NutchTool implements Tool {
	public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
	public static final String GENERATOR_MIN_SCORE = "generate.min.score";
	public static final String GENERATOR_FILTER = "generate.filter";
	public static final String GENERATOR_NORMALISE = "generate.normalise";
	public static final String GENERATOR_MAX_COUNT = "generate.max.count";
	public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
	public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
	public static final String GENERATOR_COUNT_VALUE_HOST = "host";
	public static final String GENERATOR_COUNT_VALUE_IP = "ip";
	public static final String GENERATOR_COUNT_VALUE_CFG = "cfg";
	public static final String GENERATOR_TOP_N = "generate.topN";
	public static final String GENERATOR_CUR_TIME = "generate.curTime";
	public static final String GENERATOR_DELAY = "crawl.gen.delay";
	public static final String GENERATOR_RANDOM_SEED = "generate.partition.seed";
	public static final String BATCH_ID = "generate.batch.id";

	private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.FETCH_TIME);
		FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
		FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
		FIELDS.add(WebPage.Field.SCORE);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.FETCH_INTERVAL);
		FIELDS.add(WebPage.Field.CRAWLTYPE);
		// FIELDS.add(WebPage.Field.CONFIGURL);
	}

	public static final Logger LOG = LoggerFactory.getLogger(GeneratorJob.class);

	public static class SelectorEntry implements WritableComparable<SelectorEntry> {

		String url;
		float score;
		int cfgId;
		int onceCount;

		public SelectorEntry() {
		}

		public SelectorEntry(String url, float score) {
			this.url = url;
			this.score = score;
		}

		public void readFields(DataInput in) throws IOException {
			url = Text.readString(in);
			score = in.readFloat();
			cfgId = in.readInt();
			onceCount = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			Text.writeString(out, url);
			out.writeFloat(score);
			out.writeInt(cfgId);
			out.writeInt(onceCount);
		}

		public int compareTo(SelectorEntry se) {
			if (se.score > score)
				return 1;
			else if (se.score == score)
				return url.compareTo(se.url);
			return -1;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + url.hashCode();
			result = prime * result + Float.floatToIntBits(score);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			SelectorEntry other = (SelectorEntry) obj;
			if (!url.equals(other.url))
				return false;
			if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
				return false;
			return true;
		}

		/**
		 * Sets url with score on this writable. Allows for writable reusing.
		 * 
		 * @param url
		 * @param score
		 */
		public void set(String url, float score) {
			this.url = url;
			this.score = score;
		}
	}

	public static class SelectorEntryComparator extends WritableComparator {
		public SelectorEntryComparator() {
			super(SelectorEntry.class, true);
		}
	}

	static {
		WritableComparator.define(SelectorEntry.class, new SelectorEntryComparator());
	}

	public GeneratorJob() {

	}

	public GeneratorJob(Configuration conf) {
		setConf(conf);
	}

	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		// map to inverted subset due for fetch, sort by score
		Long topN = (Long) args.get(Nutch.ARG_TOPN);
		Long curTime = (Long) args.get(Nutch.ARG_CURTIME);
		if (curTime == null) {
			curTime = System.currentTimeMillis();
		}
		if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.BatchNode.generatNode, null, LOG, true) == 0)
			return null;
		Boolean filter = (Boolean) args.get(Nutch.ARG_FILTER);
		Boolean norm = (Boolean) args.get(Nutch.ARG_NORMALIZE);
		// map to inverted subset due for fetch, sort by score
		getConf().setLong(GENERATOR_CUR_TIME, curTime);
		if (topN != null)
			getConf().setLong(GENERATOR_TOP_N, topN);
		if (filter != null)
			getConf().setBoolean(GENERATOR_FILTER, filter);
		int randomSeed = Math.abs(new Random().nextInt());
		batchId = (curTime / 1000) + "-" + randomSeed;
		getConf().setInt(GENERATOR_RANDOM_SEED, randomSeed);
		getConf().set(BATCH_ID, batchId);
		getConf().setLong(Nutch.GENERATE_TIME_KEY, System.currentTimeMillis());
		if (norm != null)
			getConf().setBoolean(GENERATOR_NORMALISE, norm);
		String mode = getConf().get(GENERATOR_COUNT_MODE, GENERATOR_COUNT_VALUE_HOST);
		if (GENERATOR_COUNT_VALUE_HOST.equalsIgnoreCase(mode)) {
			getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
		} else if (GENERATOR_COUNT_VALUE_DOMAIN.equalsIgnoreCase(mode)) {
			getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_DOMAIN);
		} else if (GENERATOR_COUNT_VALUE_IP.equalsIgnoreCase(mode)) {
			getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_IP);
		} else if (GENERATOR_COUNT_VALUE_CFG.equalsIgnoreCase(mode)) {
			getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_CFG);
		} else {
			getConf().set(GENERATOR_COUNT_MODE, "url");
			getConf().set(URLPartitioner.PARTITION_MODE_KEY, "url");
		}
		numJobs = 1;
		currentJobNum = 0;
		NutchConstant.setUrlConfig(this.getConf(), 1);
		String gids = getConf().get(NutchConstant.configIdsKey, "all");
		currentJob = new NutchJob(getConf(), "[" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)) + "]generate[" + gids + "]: " + "#"
				+ batchId);
		StorageUtils.initMapperJob(currentJob, FIELDS, WebPage.class, SelectorEntry.class, WebPage.class, GeneratorMapper.class,
				SelectorEntryPartitioner.class, false);
		StorageUtils.initReducerJob(currentJob, WebPageIndex.class, GeneratorReducer.class);
		// currentJob.setGroupingComparatorClass(SelectorEntryComparator.class);
		System.out.println("begon to submit job");
		currentJob.waitForCompletion(true);
		ToolUtil.recordJobStatus(null, currentJob, results);
		NutchConstant.preparEndJob(this.getConf(), NutchConstant.BatchNode.generatNode, LOG);
		results.put(BATCH_ID, batchId);
		return results;
	}

	private String batchId;

	/**
	 * Mark URLs ready for fetching.
	 * 
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * */
	public String generate(long topN, long curTime, boolean filter, boolean norm) throws Exception {

		LOG.info("GeneratorJob: Selecting best-scoring urls due for fetch.");
		LOG.info("GeneratorJob: starting");
		LOG.info("GeneratorJob: filtering: " + filter);
		if (topN != Long.MAX_VALUE) {
			LOG.info("GeneratorJob: topN: " + topN);
		}
		Object res = run(ToolUtil.toArgMap(Nutch.ARG_TOPN, topN, Nutch.ARG_CURTIME, curTime, Nutch.ARG_FILTER, filter, Nutch.ARG_NORMALIZE,
				norm));
		batchId = getConf().get(BATCH_ID);
		LOG.info("GeneratorJob: done");
		LOG.info("GeneratorJob: generated batch id: " + batchId);
		if (res == null)
			return null;
		return batchId;
	}

	public int run(String[] args) throws Exception {
		long curTime = System.currentTimeMillis(), topN = Long.MAX_VALUE;
		boolean filter = true, norm = true;
		String usage = "Usage: GeneratorJob <-batch batchId> [-crawlId <id>] [-topN N] [-noFilter] [-noNorm] [-force] [-cfgs <ids>]\n";

		if (args.length == 0) {
			System.err.println(usage);
			return -1;
		}
		for (int i = 0; i < args.length; i++) {
			if ("-topN".equals(args[i])) {
				topN = Long.parseLong(args[++i]);
			} else if ("-noFilter".equals(args[i])) {
				filter = false;
			} else if ("-noNorm".equals(args[i])) {
				norm = false;
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-cfgs".equals(args[i])) {
				getConf().set(NutchConstant.configIdsKey, args[++i]);
			} else if ("-batch".equals(args[i])) {
				String batchId = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId != null && !batchId.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId);
				}
				i++;
			} else if ("-force".equals(args[i])) {
				getConf().setBoolean(NutchConstant.STEP_FORCE_RUN_KEY, true);
			}
		}
		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		try {
			return (generate(topN, curTime, filter, norm) == null ? -1 : 0);
		} catch (Exception e) {
			LOG.error("GeneratorJob: " + StringUtils.stringifyException(e));
			return -1;
		}
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorJob(), args);
		System.exit(res);
	}
}
