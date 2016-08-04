package org.apache.nutch.parse.html;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get all the links page address, including: the js, html, etc. within
 * @author wanghao
 *
 */
public class MergeOutLinkPlugin {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html.AddJsOutLinkPlugin");
	public Pattern[] pattern = null;
	private Outlink[] outlink;
	private String content;
	private String urlPattern = "";
	private String[] value = null;
	
	public MergeOutLinkPlugin(Outlink[] outlinks, String content, WebPage page) {
		this.outlink = outlinks;
		this.content = content;
		this.urlPattern = page.nodeConfig.getConf("parase.plugin.outlink.name.merge");
		if (urlPattern == null || urlPattern.trim().length()<=0){
			LOG.warn("param 'parase.plugin.outlink.name.merge' is null");
			return;
		}
		String ups[] = this.urlPattern.split("~~");
		this.pattern = new Pattern[ups.length];
		this.value = new String[ups.length];
		for (int i = 0; i < ups.length; i++) {
			String up[] = this.urlPattern.split("~");
			if (up.length !=2 ){
				LOG.warn("param 'parase.plugin.outlink.name.merge' should contain pattern and value");
				continue;
			}
			this.pattern[i] = Pattern.compile(up[0]);
			this.value[i] = up[1];
			LOG.info("MergeUrlPattern------------pattern:"+up[0] +", var:"+ up[1]);
		}
	}

	public Outlink[] getOutLink(){
		if (pattern == null){
			return this.outlink;
		}
		
		for (int i = 0; i < pattern.length; i++) {
			this.outlink = getPerOutLink(this.pattern[i], this.value[i]);
		}
		
		return this.outlink;
	}
	
	public Outlink[] getPerOutLink(Pattern pattern, String value){
		if (null == content || content.length()<=0 || pattern == null){
			return this.outlink;
		}
		
		if (this.outlink == null){
			this.outlink = new Outlink[0];
		}
		
		Set<String> onlyUrlSet = new HashSet<String>();
		List<Outlink> lstOutLink = new ArrayList<Outlink>();
		for (Outlink ol : this.outlink) {
			lstOutLink.add(ol);
			onlyUrlSet.add(ol.getToUrl());
		}
		System.out.println("urlPattern="+urlPattern+",  content.length="+content.length());
		Matcher m = pattern.matcher(content);
		int tempIndex = 0;
		while (m != null && m.find()) {
			int pos = m.end();
			if (pos < 0 || pos >= content.length())
				break;
			content = content.substring(m.end());
			String url = NutchConstant.ReplaceRegex(m, value);
			if (!onlyUrlSet.contains(url)){
				try {
					LOG.info("------------extend-url="+url);
					lstOutLink.add(new Outlink(url, tempIndex+""));
				} catch (MalformedURLException e) {
					// do not care
				}
			}else{
				onlyUrlSet.add(url);
			}
			m = pattern.matcher(content);
			tempIndex++;
		}
		
		return lstOutLink.toArray(new Outlink[lstOutLink.size()]);
	}
}
