package org.apache.nutch.parse.element;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.nutch.indexer.solr.segment.SegmentSolrIndexUtil;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.storage.WebPageSegmentIndex;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class SegParserReducer extends GoraReducer<Text, WebPageIndex, String, WebPageSegment> {
	private SegMentParsers parses;
	private String batchID;
	private String batchTime;
	int rowCount = 0;
	int failCount = 0;
	SolrWriter solrWriter = null;
	boolean indexed = true;
	public DataStore<String, WebPageSegmentIndex> store = null;
	private URLNormalizers normalizers;
	public static final Logger LOG = LoggerFactory.getLogger(SegParserReducer.class);

	@Override
	protected void reduce(Text key, Iterable<WebPageIndex> values, Context context) throws IOException, InterruptedException {
		String urlKey = key.toString();
		String unreverseKey = TableUtil.unreverseUrl(urlKey);
		context.setStatus(getParseStatus(context));
		for (WebPageIndex page : values) {

			WebPageSegment wps = parseSegMent(parses, unreverseKey, page);// 有解析成功的数据返回
			if (wps != null) {
				HashMap<String, WebPageSegment> cacheWps = wps.subWps;
				wps.subWps = null;
				rowCount++;
				if (indexed) {
					NutchDocument doc = SegmentSolrIndexUtil.index(urlKey, wps);
					if (doc != null) {
						SegParserJob.LOG.info(urlKey + " url:" + unreverseKey + " write to solr");
						solrWriter.write(doc);
						wps.setMarker(new Utf8(batchID + ":" + batchTime));
					} else {
						SegParserJob.LOG.warn(urlKey + " url:" + unreverseKey + " doc is null");
					}
				}
				wps.setMarker(new Utf8(batchID + ":" + batchTime));
				context.write(urlKey, wps);
				String wpIndexReversedUrl = NutchConstant.getWebPageIndexUrl(batchID, urlKey);
				store.put(wpIndexReversedUrl, new WebPageSegmentIndex(wps));
				if (null != cacheWps && cacheWps.size()>0){
					for (String rowkey : cacheWps.keySet()) {
						int spindex = rowkey.indexOf(' ');
						if (spindex > 0) {
							rowkey = rowkey.substring(0, spindex);
						}
						spindex = rowkey.indexOf('>');
						if (spindex > 0) {
							rowkey = rowkey.substring(0, spindex);
						}
						spindex = rowkey.indexOf('<');
						if (spindex > 0) {
							rowkey = rowkey.substring(0, spindex);
						}
						String url = normalizers.normalize(rowkey, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
						URI u;
						try {
							u = new URI(url);
							InetAddress.getByName(u.getHost());
							String reverseKey = TableUtil.reverseUrl(url);
							wps = cacheWps.get(rowkey);
							context.write(reverseKey, wps);
							wpIndexReversedUrl = NutchConstant.getWebPageIndexUrl(batchID, reverseKey);
							store.put(wpIndexReversedUrl, new WebPageSegmentIndex(wps));
						} catch (URISyntaxException e) {
							LOG.error("写子链接属性值错误：link=" + rowkey, e);
						} catch (MalformedURLException e) {
							LOG.error("地址反转出错：link=" + rowkey, e);
						} catch (Exception e) {
							LOG.error("其他错误：link=" + rowkey, e);
						}
					}
				}
			} else {
				failCount++;
			}
		}
	}

	public static WebPageSegment parseSegMent(SegMentParsers parses, String unreverseKey, WebPage page) {
		if (page.getContent() == null) {
			System.err.println("内容为空：" + unreverseKey);
			return null;
		} else if (page.getTitle() == null) {
			System.err.println("标题为空：" + unreverseKey);
			return null;
		} else if (page.getText() == null) {
			System.err.println("文本为空：" + unreverseKey);
			return null;
		} else if (page.getConfigUrl() == null) {
			System.err.println("配置地址为空：" + unreverseKey);
			return null;
		}
		WebPageSegment wps = new WebPageSegment();
		boolean res = false;
		try {
			res = parses.parser(unreverseKey, page, wps);
			System.out.println("解析 完成：" + unreverseKey);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		wps.rootNode = null;
		wps.content = null;
		// 有解析成功的数据返回
		if (res == false || wps.getConfigUrl() == null) {
			return null;
		}
		wps.setBaseUrl(new Utf8(unreverseKey));
		wps.setScore(page.getScore());
		wps.setFetchTime(page.getPrevFetchTime());
		wps.setParseTime(System.currentTimeMillis());
		Map<Utf8, Utf8> segCnt = wps.getSegMentCnt();
		if (segCnt.containsKey(WebPageSegment.topicTypeIdColName)) {
			try {
				long topic = Long.parseLong(segCnt.get(WebPageSegment.topicTypeIdColName).toString());
				wps.setTopicTypeId(topic);
			} catch (Exception e) {
			}
		}

		if (segCnt.containsKey(WebPageSegment.titleColName))
			wps.setTitle(segCnt.get(WebPageSegment.titleColName));
		else
			wps.setTitle(page.getTitle());
		boolean setDataTime = false;
		if (segCnt.containsKey(WebPageSegment.dataTimeColName)) {
			try {
				String strDt = segCnt.get(WebPageSegment.dataTimeColName).toString();
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				wps.setDataTime(df.parse(strDt).getTime());
				setDataTime = true;
			} catch (Exception e) {
			}
		}
		if (!setDataTime) {
			Utf8 mdt = page.getFromHeaders(new Utf8(org.apache.nutch.metadata.HttpHeaders.LAST_MODIFIED));
			if (mdt != null) {
				try {
					wps.setDataTime(Date.parse(mdt.toString()));
					setDataTime = true;
				} catch (Exception e) {

				}
			}
		}
		// if (!setDataTime) {
		// long md = page.getModifiedTime();
		// if (md != 0) {
		// setDataTime = true;
		// wps.setDataTime(md);
		// }
		// }
		// if (!setDataTime)
		// wps.setDataTime(page.getPrevFetchTime());
		if (!segCnt.containsKey(WebPageSegment.keyworksColName)) {
			ByteBuffer bf = page.getFromMetadata(new Utf8(Metadata.KEYWORDS));
			if (bf != null)
				segCnt.put(WebPageSegment.keyworksColName, new Utf8(bf.array()));
		}
		if (!segCnt.containsKey(WebPageSegment.descriptionColName)) {
			ByteBuffer bf = page.getFromMetadata(new Utf8(Metadata.DESCRIPTION));
			if (bf != null)
				segCnt.put(WebPageSegment.descriptionColName, new Utf8(bf.array()));
		}
		// Map<Utf8, Utf8> extendInfoAttrs;// 信息属性类型ID
		// Map<Utf8, Utf8> segMentAttr;// 内容要素二级属性 标签名=segmeng_col-segmeng_attr_col:value
		// Map<Utf8, Utf8> segMentCnt;// 内容要素 segmeng_col,segmentContent
		// Map<Utf8, Utf8> segMentKeys;// 分词关键字
		// Map<Utf8, Utf8> unionUrls;// 内容相关联的URL url,title
		// Map<Utf8, Utf8> markers;

		return wps;
	}

	String getParseStatus(Context context) {
		return "成功解析到网页内容要素的页面数:" + rowCount + "  " + " 失败数:" + failCount + " <br/>更新时间：" + (new Date().toLocaleString());
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		solrWriter.close();
		store.close();
		context.setStatus("成功解析到网页内容要素的页面数：" + rowCount + "  失败数：" + failCount);
		FetcherJob.LOG.info("parserReducer-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context
				.getTaskAttemptID().getTaskID().toString(), false, true, rowCount);
	};

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		parses = new SegMentParsers(conf);
		normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		NutchConstant.getSegmentParseRules(conf);
		indexed = conf.getBoolean(SegParserJob.SEGMENT_INDEX_KEY, false);
		solrWriter = new SolrWriter();
		solrWriter.open(conf);
		try {
			store = StorageUtils.createWebStore(conf, String.class, WebPageSegmentIndex.class);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		SegParserJob.LOG.info("solrIndexUrl:" + conf.get(SolrConstants.SERVER_URL));
		SegParserJob.LOG.info("parserReducer-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context, false);
	}
}
