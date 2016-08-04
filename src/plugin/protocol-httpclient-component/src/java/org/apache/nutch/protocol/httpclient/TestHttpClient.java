package org.apache.nutch.protocol.httpclient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.junit.Test;

public class TestHttpClient {

	public static void main(String[] args) throws IOException {
		// 测试服务端登录成功情况loginType = 1(包含ssl)
		// testDefaultServer1();// 默认服务
		// testDefaultServer2();// 默认Servlet服务 / 特殊服务端服务
		// // 测试客户端登录成功情况loginType= 2(包含ssl)
		// testDefaultClient1();// 默认服务（需要js登录）
		// testDefaultClient2();// 无需登录
		// // 测试服务端请求数据失败的情况下，使用客户端登录请求返回信息loginType= 1>2
		// testDefaultServerClient1();// 默认服务

		BufferedReader reader = new BufferedReader(new FileReader(
				"E:/myeclipse/workspace/apache-nutch-2.1/src/plugin/protocol-httpclient-component/src/java/qqmd52.js"));
		String temp = null;
		StringBuilder str = new StringBuilder();
		while ((temp = reader.readLine()) != null) {
			str.append(temp);
		}
		System.out.println(str.toString());

		String js = "var hexcase = 1;var b64pad = \"\";var chrsz = 8;var mode = 32;function md5(A) {	return hex_md5(A)}function hex_md5(A) {	return binl2hex(core_md5(str2binl(A), A.length * chrsz))}function str_md5(A) {	return binl2str(core_md5(str2binl(A), A.length * chrsz))}function hex_hmac_md5(A, B) {	return binl2hex(core_hmac_md5(A, B))}function b64_hmac_md5(A, B) {	return binl2b64(core_hmac_md5(A, B))}function str_hmac_md5(A, B) {	return binl2str(core_hmac_md5(A, B))}function core_md5(K, F) {	K[F >> 5] |= 128 << ((F) % 32);	K[(((F + 64) >>> 9) << 4) + 14] = F;	var J = 1732584193;	var I = -271733879;	var H = -1732584194;	var G = 271733878;	for (var C = 0; C < K.length; C += 16) {		var E = J;		var D = I;		var B = H;		var A = G;		J = md5_ff(J, I, H, G, K[C + 0], 7, -680876936);		G = md5_ff(G, J, I, H, K[C + 1], 12, -389564586);		H = md5_ff(H, G, J, I, K[C + 2], 17, 606105819);		I = md5_ff(I, H, G, J, K[C + 3], 22, -1044525330);		J = md5_ff(J, I, H, G, K[C + 4], 7, -176418897);		G = md5_ff(G, J, I, H, K[C + 5], 12, 1200080426);		H = md5_ff(H, G, J, I, K[C + 6], 17, -1473231341);		I = md5_ff(I, H, G, J, K[C + 7], 22, -45705983);		J = md5_ff(J, I, H, G, K[C + 8], 7, 1770035416);		G = md5_ff(G, J, I, H, K[C + 9], 12, -1958414417);		H = md5_ff(H, G, J, I, K[C + 10], 17, -42063);		I = md5_ff(I, H, G, J, K[C + 11], 22, -1990404162);		J = md5_ff(J, I, H, G, K[C + 12], 7, 1804603682);		G = md5_ff(G, J, I, H, K[C + 13], 12, -40341101);		H = md5_ff(H, G, J, I, K[C + 14], 17, -1502002290);		I = md5_ff(I, H, G, J, K[C + 15], 22, 1236535329);		J = md5_gg(J, I, H, G, K[C + 1], 5, -165796510);		G = md5_gg(G, J, I, H, K[C + 6], 9, -1069501632);		H = md5_gg(H, G, J, I, K[C + 11], 14, 643717713);		I = md5_gg(I, H, G, J, K[C + 0], 20, -373897302);		J = md5_gg(J, I, H, G, K[C + 5], 5, -701558691);		G = md5_gg(G, J, I, H, K[C + 10], 9, 38016083);		H = md5_gg(H, G, J, I, K[C + 15], 14, -660478335);		I = md5_gg(I, H, G, J, K[C + 4], 20, -405537848);		J = md5_gg(J, I, H, G, K[C + 9], 5, 568446438);		G = md5_gg(G, J, I, H, K[C + 14], 9, -1019803690);		H = md5_gg(H, G, J, I, K[C + 3], 14, -187363961);		I = md5_gg(I, H, G, J, K[C + 8], 20, 1163531501);		J = md5_gg(J, I, H, G, K[C + 13], 5, -1444681467);		G = md5_gg(G, J, I, H, K[C + 2], 9, -51403784);		H = md5_gg(H, G, J, I, K[C + 7], 14, 1735328473);		I = md5_gg(I, H, G, J, K[C + 12], 20, -1926607734);		J = md5_hh(J, I, H, G, K[C + 5], 4, -378558);		G = md5_hh(G, J, I, H, K[C + 8], 11, -2022574463);		H = md5_hh(H, G, J, I, K[C + 11], 16, 1839030562);		I = md5_hh(I, H, G, J, K[C + 14], 23, -35309556);		J = md5_hh(J, I, H, G, K[C + 1], 4, -1530992060);		G = md5_hh(G, J, I, H, K[C + 4], 11, 1272893353);		H = md5_hh(H, G, J, I, K[C + 7], 16, -155497632);		I = md5_hh(I, H, G, J, K[C + 10], 23, -1094730640);		J = md5_hh(J, I, H, G, K[C + 13], 4, 681279174);		G = md5_hh(G, J, I, H, K[C + 0], 11, -358537222);		H = md5_hh(H, G, J, I, K[C + 3], 16, -722521979);		I = md5_hh(I, H, G, J, K[C + 6], 23, 76029189);		J = md5_hh(J, I, H, G, K[C + 9], 4, -640364487);		G = md5_hh(G, J, I, H, K[C + 12], 11, -421815835);		H = md5_hh(H, G, J, I, K[C + 15], 16, 530742520);		I = md5_hh(I, H, G, J, K[C + 2], 23, -995338651);		J = md5_ii(J, I, H, G, K[C + 0], 6, -198630844);		G = md5_ii(G, J, I, H, K[C + 7], 10, 1126891415);		H = md5_ii(H, G, J, I, K[C + 14], 15, -1416354905);		I = md5_ii(I, H, G, J, K[C + 5], 21, -57434055);		J = md5_ii(J, I, H, G, K[C + 12], 6, 1700485571);		G = md5_ii(G, J, I, H, K[C + 3], 10, -1894986606);		H = md5_ii(H, G, J, I, K[C + 10], 15, -1051523);		I = md5_ii(I, H, G, J, K[C + 1], 21, -2054922799);		J = md5_ii(J, I, H, G, K[C + 8], 6, 1873313359);		G = md5_ii(G, J, I, H, K[C + 15], 10, -30611744);		H = md5_ii(H, G, J, I, K[C + 6], 15, -1560198380);		I = md5_ii(I, H, G, J, K[C + 13], 21, 1309151649);		J = md5_ii(J, I, H, G, K[C + 4], 6, -145523070);		G = md5_ii(G, J, I, H, K[C + 11], 10, -1120210379);		H = md5_ii(H, G, J, I, K[C + 2], 15, 718787259);		I = md5_ii(I, H, G, J, K[C + 9], 21, -343485551);		J = safe_add(J, E);		I = safe_add(I, D);		H = safe_add(H, B);		G = safe_add(G, A)	}	if (mode == 16) {		return Array(I, H)	} else {		return Array(J, I, H, G)	}}function md5_cmn(F, C, B, A, E, D) {	return safe_add(bit_rol(safe_add(safe_add(C, F), safe_add(A, D)), E), B)}function md5_ff(C, B, G, F, A, E, D) {	return md5_cmn((B & G) | ((~B) & F), C, B, A, E, D)}function md5_gg(C, B, G, F, A, E, D) {	return md5_cmn((B & F) | (G & (~F)), C, B, A, E, D)}function md5_hh(C, B, G, F, A, E, D) {	return md5_cmn(B ^ G ^ F, C, B, A, E, D)}function md5_ii(C, B, G, F, A, E, D) {	return md5_cmn(G ^ (B | (~F)), C, B, A, E, D)}function core_hmac_md5(C, F) {	var E = str2binl(C);	if (E.length > 16) {		E = core_md5(E, C.length * chrsz)	}	var A = Array(16), D = Array(16);	for (var B = 0; B < 16; B++) {		A[B] = E[B] ^ 909522486;		D[B] = E[B] ^ 1549556828	}	var G = core_md5(A.concat(str2binl(F)), 512 + F.length * chrsz);	return core_md5(D.concat(G), 512 + 128)}function safe_add(A, D) {	var C = (A & 65535) + (D & 65535);	var B = (A >> 16) + (D >> 16) + (C >> 16);	return (B << 16) | (C & 65535)}function bit_rol(A, B) {	return (A << B) | (A >>> (32 - B))}function str2binl(D) {	var C = Array();	var A = (1 << chrsz) - 1;	for (var B = 0; B < D.length * chrsz; B += chrsz) {		C[B >> 5] |= (D.charCodeAt(B / chrsz) & A) << (B % 32)	}	return C}function binl2str(C) {	var D = \"\";	var A = (1 << chrsz) - 1;	for (var B = 0; B < C.length * 32; B += chrsz) {		D += String.fromCharCode((C[B >> 5] >>> (B % 32)) & A)	}	return D}function binl2hex(C) {	var B = hexcase ? \"0123456789ABCDEF\" : \"0123456789abcdef\";	var D = \"\";	for (var A = 0; A < C.length * 4; A++) {		D += B.charAt((C[A >> 2] >> ((A % 4) * 8 + 4)) & 15)				+ B.charAt((C[A >> 2] >> ((A % 4) * 8)) & 15)	}	return D}function binl2b64(D) {	var C = \"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/\";	var F = \"\";	for (var B = 0; B < D.length * 4; B += 3) {		var E = (((D[B >> 2] >> 8 * (B % 4)) & 255) << 16)				| (((D[B + 1 >> 2] >> 8 * ((B + 1) % 4)) & 255) << 8)				| ((D[B + 2 >> 2] >> 8 * ((B + 2) % 4)) & 255);		for (var A = 0; A < 4; A++) {			if (B * 8 + A * 6 > D.length * 32) {				F += b64pad			} else {				F += C.charAt((E >> 6 * (3 - A)) & 63)			}		}	}	return F}function hexchar2bin(str) {	var arr = [];	for (var i = 0; i < str.length; i = i + 2) {		arr.push(\"\\x\" + str.substr(i, 2))	}	arr = arr.join(\"\");	eval(\"var temp = '\" + arr + \"'\");	return temp}function QXWEncodePwd(u, p, v) {	var I = hexchar2bin(md5(p));	var H = md5(I + TTescapechar2bin(u));	var G = md5(H + v.toUpperCase());	return G}function TTescapechar2bin(str) {	eval(\"var temp = '\" + str + \"'\");	return temp}";
		System.out.println(js);
	}

	@Test
	public void testDefaultServer1() {
		// 测试完成（1、登录，2、未登录情况）
		WebPage page = new WebPage();
		page.nodeConfig = new UrlNodeConfig();
		String url = "https://localhost:8443/manager/html/list";// https://localhost:8443/manager/html/list
		String loginAddress = "https://localhost:8443/manager/";// https://localhost:8443/manager/
		String loginClass = "";
		String loginjs = "";
		String loginoutAddress = "";
		String loginPassword = "s3cret";
		String loginUserName = "tomcat";
		String loginValidateAddress = "";
		boolean needLogin = true;// 登录
		int crawlType = 1;
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("", "");
		request(paramMap, loginAddress, loginClass, loginjs, loginoutAddress, loginPassword, crawlType, loginUserName,
				loginValidateAddress, needLogin, page, url);
	}

	@Test
	public void testDefaultServer2() {
		// 测试完成（1、登录，2、未登录情况, 3、Session由DefaultHttpClient自身保存）
		WebPage page = new WebPage();
		page.nodeConfig = new UrlNodeConfig();
		String url = "http://localhost:8080/ServletApp/login.do";
		String loginAddress = "http://localhost:8080/ServletApp/login.do";
		// org.apache.nutch.protocol.httpclient.site.HttpResponseComponentSina
		String loginClass = "org.apache.nutch.protocol.httpclient.site.HttpResponseComponentServlet";
		String loginjs = "";
		String loginoutAddress = "";
		String loginPassword = "123";
		String loginUserName = "www.xin126.cn";
		String loginValidateAddress = "";
		boolean needLogin = true;
		int crawlType = 1;
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("username", "www.xin126.cn");
		paramMap.put("password", "123");
		request(paramMap, loginAddress, loginClass, loginjs, loginoutAddress, loginPassword, crawlType, loginUserName,
				loginValidateAddress, needLogin, page, url);
	}

	@Test
	public void testDefaultClient1() {
		WebPage page = new WebPage();
		page.nodeConfig = new UrlNodeConfig();
		String url = "http://account.weibo.com/set/index";
		String loginAddress = "http://weibo.com/login";
		String loginClass = "org.apache.nutch.protocol.httpclient.site.HttpResponseComponentSina";
		String loginjs = "var div = document.getElementById('login_form');var loginname = document.getElementsByName('loginname');for ( var i = 0; i < loginname.length; i++) {	loginname[i].setAttribute('value', '939636293@qq.com');	loginname[i].value = '939636293@qq.com';};var password = document.getElementsByName('password');for ( var i = 0; i < password.length; i++) {	password[i].setAttribute('value', 'abc753951');	var av = password[i].value = 'abc753951';};var alist = div.getElementsByTagName('A');for ( var i = 0; i < alist.length; i++) {	if ('W_btn_d' == alist[i].className) {		var e = document.createEvent('HTMLEvents');		e.initEvent('click', false, false);		var res = alist[i].dispatchEvent(e);	}}";
		String loginoutAddress = "[\"http://weibo.com\",\"http://weibo.com/logout.php\",\"http://login.sina.com.cn\",\"http://weibo.com/login\"]";
		String loginPassword = "1QAZ-PL,";
		String loginUserName = "939636293@qq.com";
		String loginValidateAddress = "http://account.weibo.com/set/index";
		boolean needLogin = true;
		int crawlType = 2;
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("loginname", "939636293@qq.com");
		paramMap.put("password", "1QAZ-PL,");
		request(paramMap, loginAddress, loginClass, loginjs, loginoutAddress, loginPassword, crawlType, loginUserName,
				loginValidateAddress, needLogin, page, url);
	}

	@Test
	public void testDefaultClient2() {
		WebPage page = new WebPage();
		page.nodeConfig = new UrlNodeConfig();
		String url = "http://www.sina.com.cn";
		String loginAddress = "";
		String loginClass = "";
		String loginjs = "";
		String loginoutAddress = "";
		String loginPassword = "";
		String loginUserName = "";
		String loginValidateAddress = "";
		boolean needLogin = true;
		int crawlType = 2;
		Map<String, String> paramMap = new HashMap<String, String>();
		request(paramMap, loginAddress, loginClass, loginjs, loginoutAddress, loginPassword, crawlType, loginUserName,
				loginValidateAddress, needLogin, page, url);
	}

	@Test
	public void testDefaultServerClient1() {
		WebPage page = new WebPage();
		page.nodeConfig = new UrlNodeConfig();
		String url = "http://account.weibo.com/set/index";
		String loginAddress = "http://weibo.com/login";
		String loginClass = "org.apache.nutch.protocol.httpclient.site.HttpResponseComponentSina";
		// String loginjs =
		// "var div = document.getElementById('login_form');var loginname = document.getElementsByName('loginname');for ( var i = 0; i < loginname.length; i++) {	loginname[i].setAttribute('value', '939636293@qq.com');	loginname[i].value = '939636293@qq.com';};var password = document.getElementsByName('password');for ( var i = 0; i < password.length; i++) {	password[i].setAttribute('value', 'abc753951');	var av = password[i].value = 'abc753951';};var alist = div.getElementsByTagName('A');for ( var i = 0; i < alist.length; i++) {	if ('W_btn_d' == alist[i].className) {		var e = document.createEvent('HTMLEvents');		e.initEvent('click', false, false);		var res = alist[i].dispatchEvent(e);	}}";
		String loginjs = "";
		String loginoutAddress = "[\"http://weibo.com\",\"http://weibo.com/logout.php\",\"http://login.sina.com.cn\",\"http://weibo.com/login\"]";
		String loginPassword = "1QAZ-PL,";
		String loginUserName = "939636293@qq.com";
		String loginValidateAddress = "http://account.weibo.com/set/index";
		boolean needLogin = true;
		int crawlType = 3;
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("loginname", "939636293@qq.com");
		paramMap.put("password", "1QAZ-PL,");
		request(paramMap, loginAddress, loginClass, loginjs, loginoutAddress, loginPassword, crawlType, loginUserName,
				loginValidateAddress, needLogin, page, url);
	}

	public void request(Map<String, String> paramMap, String loginAddress, String loginClass, String loginjs, String loginoutAddress,
			String loginPassword, int crawlType, String loginUserName, String loginValidateAddress, boolean needLogin, WebPage page,
			String url) {
		page.nodeConfig.loginAddress = loginAddress;
		page.nodeConfig.loginCLASS = loginClass;
		page.nodeConfig.loginJS = loginjs;
		page.nodeConfig.loginoutAddress = loginoutAddress;
		page.nodeConfig.loginPassword = loginPassword;
		page.nodeConfig.loginUserName = loginUserName;
		page.nodeConfig.loginValidateAddress = loginValidateAddress;
		page.nodeConfig.loginParam = paramMap;
		page.nodeConfig.needLogin = needLogin;
		page.setCrawlType(crawlType);

		Protocol protocol = new HttpComponent();
		ProtocolOutput output = protocol.getProtocolOutput(url, page);
		ProtocolStatus status = output.getStatus();
		Content content = output.getContent();
		System.out.println("status:" + status);
		System.out.println(content.toString());
	}
}
