package org.apache.nutch.parse.html;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get all the links page address, including: the js, html, etc. within
 * @author wanghao
 *
 */
public class FilterSpecialOutLinkPlugin {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html.FilterSpecialOutLinkPlugin");
	private Outlink[] outlink;
	private Pattern[] urlPattern = null;
	private Pattern[] filterPattern = null;
	private String url=null;
	
	public FilterSpecialOutLinkPlugin(Outlink[] outlinks, WebPage page) {
		this.outlink = outlinks;
		this.url = page.getBaseUrl() != null?page.getBaseUrl().toString():null;
		String param = page.nodeConfig.getConf("parase.plugin.outlink.name.filter");
		if (param == null || param.trim().length()<=0){
			LOG.warn("param 'parase.plugin.outlink.name.filter' is null");
			return;
		}
		
		LOG.info("url:"+this.url);
		String ups[] = param.split("~~");
		if (ups.length !=2 ){
			LOG.warn("param 'parase.plugin.outlink.name.filter' should contain pattern and value");
			return;
		}
		
		// url地址
		String up[] = ups[0].split("~");
		this.urlPattern = new Pattern[up.length];
		for (int i = 0; i < up.length; i++) {
			if (up[i] == null || up[i].trim().length()<=0){
				continue;
			}
			this.urlPattern[i] = Pattern.compile(up[i]);
		}
		
		// 匹配outlink地址
		up = ups[1].split("~");
		this.filterPattern = new Pattern[up.length];
		for (int i = 0; i < up.length; i++) {
			if (up[i] == null || up[i].trim().length()<=0){
				continue;
			}
			this.filterPattern[i] = Pattern.compile(up[i]);
		}
	}

	public Outlink[] getOutLink(){
		boolean isPattern = false;
		for (int i = 0; i < this.urlPattern.length; i++) {
			if (this.urlPattern[i] != null && this.urlPattern[i].matcher(url).matches()){
				isPattern = true;
				break;
			}
		}
		
		if (!isPattern){
			LOG.info("Don't need to filter");
			return this.outlink;
		}
		
		List<Outlink> lstOutLink = new ArrayList<Outlink>();
		for (int i = 0; i < this.outlink.length; i++) {
			boolean isMove = false;
			for (int j = 0; j < this.filterPattern.length; j++) {
				if (this.outlink[i] == null || this.outlink[i].getToUrl() == null){
					continue;
				}
				if (null != this.filterPattern[j] && this.filterPattern[j].matcher(this.outlink[i].getToUrl()).matches()){
					LOG.info("filter_outlinkurl="+this.outlink[i].getToUrl() +", pattern:"+this.filterPattern[j]);
					isMove = true;
					break;
				}
			}
			if (isMove){
				continue;
			}
			
			lstOutLink.add(this.outlink[i]);
		}
		
		return lstOutLink.toArray(new Outlink[lstOutLink.size()]);
	}
}
