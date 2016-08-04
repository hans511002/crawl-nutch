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
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get all the links page address, including: the js, html, etc. within
 * @author wanghao
 *
 */
public class PartRangeOutLinkPlugin {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html.PartRangeOutLinkPlugin");
	public Pattern pattern = null;
	private Outlink[] outlink;
	private String content;
	private String url;
	private String range = "";
	private String beginPart;
	private String endPart;
	private Pattern urlPattern;
	private Pattern outlinkUrlPattern;
	
	public PartRangeOutLinkPlugin(Outlink[] outlinks, String content, WebPage page) {
		this.outlink = outlinks;
		this.content = content;
		this.range = page.nodeConfig.getConf("parase.plugin.outlink.name.partrange");
		this.url = page.getBaseUrl() != null?page.getBaseUrl().toString():null;
		if (range == null){
			return;
		}
		
		LOG.info("range------------:"+this.range);
		String rg[] = this.range.split("~");
		if (rg.length == 3){
			if (rg[0] != null && rg[0].trim().length()>0){
				this.urlPattern = Pattern.compile(rg[0]);
			}
			this.beginPart = rg[1];
			this.endPart = rg[2];
			this.pattern = Pattern.compile("(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?");
			LOG.info("range beginPart------------:"+this.beginPart);
			LOG.info("range endPart------------:"+this.endPart);
			return;
		}else if (rg.length == 4) {
			if (rg[0] != null && rg[0].trim().length()>0){
				this.urlPattern = Pattern.compile(rg[0]);
			}
			this.beginPart = rg[1];
			this.endPart = rg[2];
			this.outlinkUrlPattern = Pattern.compile(rg[3]);
			this.pattern = Pattern.compile("(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?");
		}
		else if (rg.length == 5) {
			if (rg[0] != null && rg[0].trim().length()>0){
				this.urlPattern = Pattern.compile(rg[0]);
			}
			this.beginPart = rg[1];
			this.endPart = rg[2];
			this.outlinkUrlPattern = Pattern.compile(rg[4]);
			this.pattern = Pattern.compile(rg[5]);
		}else{
			this.range = null;
		}
		
		LOG.info("range beginPart------------:"+this.beginPart);
		LOG.info("range endPart------------:"+this.endPart);
	}

	public Outlink[] getOutLink(){
		if (null == content || content.length()<=0 || null == this.range || null == this.urlPattern){
			return this.outlink;
		}
		
		Matcher basem = this.urlPattern.matcher(this.url);
		if (!basem.matches()){
			return this.outlink;
		}
		
		if (this.outlink == null){
			this.outlink = new Outlink[0];
		}
		
		Set<String> onlyUrlSet = new HashSet<String>();
		List<Outlink> lstOutLink = new ArrayList<Outlink>();
		System.out.println("urlPattern="+range+",  content.length="+content.length());
		
		if (this.beginPart != null && !this.beginPart.equals("")) {
			int index = TableUtil.indexOf(content, this.beginPart, true);
			int eindex = content.length();
			if (content != null && !content.equals("")) {
				eindex = TableUtil.indexOf(content, this.endPart, true);
			}
			if (index < 0)
				return this.outlink;
			if (eindex > index) {
				content = content.substring(index, eindex);
			} else {
				content = content.substring(index);
			}
		}
		String cloneContent = new String(content);
		Matcher m = pattern.matcher(content);
		int tempIndex = 0;
		while (m != null && m.find()) {
			int pos = m.end();
			if (pos < 0 || pos >= content.length())
				break;
			content = content.substring(m.end());
			String url = NutchConstant.ReplaceRegex(m, "$0");
			if (!onlyUrlSet.contains(url)){
				if (null != outlinkUrlPattern && !outlinkUrlPattern.matcher(url).matches()){
					continue;
				}
				try {
					LOG.info("--extend-url="+url);
					lstOutLink.add(new Outlink(url, tempIndex+""));
					onlyUrlSet.add(url);
				} catch (MalformedURLException e) {
					// do not care
				}
			}
			m = pattern.matcher(content);
			tempIndex++;
		}
		
		// href
		Pattern hrefPattern = Pattern.compile("href=\"([\\s\\S]*?)\"");
		Matcher m1 = hrefPattern.matcher(cloneContent);
		tempIndex = 0;
		while (m1 != null && m1.find()) {
			int pos = m1.end();
			if (pos < 0 || pos >= cloneContent.length())
				break;
			cloneContent = cloneContent.substring(m1.end());
			String url = NutchConstant.ReplaceRegex(m1, "$1");
			if (null != url){
				if (url.startsWith("http://")){
					
				}else if(url.startsWith("www.")){
					url="http://"+url;
				}else if(url.startsWith("/")){
					url=(this.url.endsWith("/")?this.url.substring(0, this.url.length()-1):this.url)+url;
				}else{
					url=this.url+"/"+url;
				}
			}
			if (!onlyUrlSet.contains(url)){
				if (null != outlinkUrlPattern && !outlinkUrlPattern.matcher(url).matches()){
					continue;
				}
				try {
					LOG.info("--href extend-url="+url);
					lstOutLink.add(new Outlink(url, tempIndex+""));
					onlyUrlSet.add(url);
				} catch (MalformedURLException e) {
					// do not care
				}
			}
			m1 = hrefPattern.matcher(cloneContent);
			tempIndex++;
		}
		
		return lstOutLink.toArray(new Outlink[lstOutLink.size()]);
	}
}
