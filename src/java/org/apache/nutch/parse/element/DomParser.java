/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse.element;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nutch.parse.html.HtmlParser;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class DomParser extends HtmlParser {

	private String defaultCharEncoding;

	public String getNodeValue(byte[] content, String domId) throws Exception {
		DocumentFragment root;
		try {
			InputSource input = new InputSource(new ByteArrayInputStream(content));
			input.setEncoding(defaultCharEncoding);
			root = parse(input);
		} catch (Exception e) {
			throw e;
		}
		StringBuilder sb = new StringBuilder();
		Node node = getNodeById(root, domId);
		if (node == null)
			return null;
		getTextHelper(sb, node);
		return sb.toString().replaceAll("<P></P>", "");
	}

	public Node getNodeById(Node node, String domId) {
		if (node == null)
			return null;
		String nodeName = node.getNodeName();
		if ("script".equalsIgnoreCase(nodeName)) {
			return null;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return null;
		}
		NamedNodeMap attr = node.getAttributes();
		if (attr != null) {
			Node id = attr.getNamedItem("id");
			if (id != null) {
				if (domId.equals(id.getNodeValue())) {
					return node;
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return null;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			attr = _node.getAttributes();
			if (attr != null) {
				Node id = attr.getNamedItem("id");
				if (id != null) {
					if (domId.equals(id.getNodeValue())) {
						return _node;
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			Node _node = getNodeById(currentChildren.item(i), domId);
			if (_node != null)
				return _node;
		}
		return null;
	}

	public List<Node> getNodeListByIdPri(Node node, String idPri) {
		List<Node> res = new ArrayList<Node>();
		getNodeListByIdPri(node, idPri, res);
		return res;
	}

	private void getNodeListByIdPri(Node node, String idPri, List<Node> res) {
		if (node == null)
			return;
		String nodeName = node.getNodeName();
		if ("script".equalsIgnoreCase(nodeName)) {
			return;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return;
		}
		NamedNodeMap attr = node.getAttributes();
		if (attr != null) {
			Node id = attr.getNamedItem("id");
			if (id != null) {
				String sid = id.getNodeValue();
				if (sid.startsWith(idPri)) {
					res.add(node);
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			attr = _node.getAttributes();
			if (attr != null) {
				Node id = attr.getNamedItem("id");
				if (id != null) {
					String sid = id.getNodeValue();
					if (sid.startsWith(idPri)) {
						res.add(node);
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			getNodeListByIdPri(currentChildren.item(i), idPri, res);
		}
	}

	public List<Node> getNodeListByRegexId(Node node, Pattern idPri) {
		List<Node> res = new ArrayList<Node>();
		getNodeListByRegexId(node, idPri, res);
		return res;
	}

	private void getNodeListByRegexId(Node node, Pattern idPri, List<Node> res) {
		if (node == null)
			return;
		String nodeName = node.getNodeName();
		if ("script".equalsIgnoreCase(nodeName)) {
			return;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return;
		}
		NamedNodeMap attr = node.getAttributes();
		if (attr != null) {
			Node id = attr.getNamedItem("id");
			if (id != null) {
				String sid = id.getNodeValue();
				Matcher m = idPri.matcher(sid);
				if (m != null && idPri.matcher(sid).find()) {
					res.add(node);
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			attr = _node.getAttributes();
			if (attr != null) {
				Node id = attr.getNamedItem("id");
				if (id != null) {
					String sid = id.getNodeValue();
					Matcher m = idPri.matcher(sid);
					if (m != null && idPri.matcher(sid).find()) {
						res.add(node);
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			getNodeListByRegexId(currentChildren.item(i), idPri, res);
		}
	}

	public Node getNodeByAttrValue(Node node, String tag, String attrName, String attrValue) {
		if (node == null)
			return null;
		String nodeName = node.getNodeName();
		if ("script".equalsIgnoreCase(nodeName)) {
			return null;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return null;
		}
		if (tag.equals(nodeName)) {
			NamedNodeMap attr = node.getAttributes();
			if (attr != null) {
				Node id = attr.getNamedItem(attrName);
				if (id != null) {
					if (attrValue.equals(id.getNodeValue())) {
						return node;
					}
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return null;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			NamedNodeMap attr = _node.getAttributes();
			if (tag.equals(_node.getNodeName()) == false)
				continue;
			if (attr != null) {
				Node id = attr.getNamedItem(attrName);
				if (id != null) {
					if (attrValue.equals(id.getNodeValue())) {
						return _node;
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			Node _node = getNodeByAttrValue(currentChildren.item(i), tag, attrName, attrValue);
			if (_node != null)
				return _node;
		}
		return null;

	}

	public Node getNodeByAttrPri(Node node, String tag, String attrName, String attrValuePri) {
		if (node == null)
			return null;
		String nodeName = node.getNodeName();
		if ("script".equalsIgnoreCase(nodeName)) {
			return null;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return null;
		}
		if (tag.equals(nodeName)) {
			NamedNodeMap attr = node.getAttributes();
			if (attr != null) {
				Node id = attr.getNamedItem(attrName);
				if (id != null) {
					String sid = id.getNodeValue();
					if (sid.startsWith(attrValuePri)) {
						return node;
					}
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return null;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			NamedNodeMap attr = _node.getAttributes();
			if (tag.equals(_node.getNodeName()) == false)
				continue;
			if (attr != null) {
				Node id = attr.getNamedItem(attrName);
				if (id != null) {
					String sid = id.getNodeValue();
					if (sid.startsWith(attrValuePri)) {
						return node;
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			Node _node = getNodeByAttrPri(currentChildren.item(i), tag, attrName, attrValuePri);
			if (_node != null)
				return _node;
		}
		return null;
	}

	// private String[][] tagPath = null;
	public Node getNodeByTagPath(Node node, String[][] tagPath) {
		Node _node = node;
		for (int i = 0; i < tagPath.length; i++) {
			_node = getNodeByTagPath(_node, tagPath, i);
			if (_node == null)
				return null;
		}
		return _node;
	}

	private Node getNodeByTagPath(Node node, String[][] tagPath, int pos) {
		if (node == null)
			return null;
		if (pos >= tagPath.length)
			return null;
		String nodeName = node.getNodeName().toUpperCase();
		if ("script".equalsIgnoreCase(nodeName)) {
			return null;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return null;
		}
		if (tagPath[pos][0].equals(nodeName)) {
			NamedNodeMap attr = node.getAttributes();
			if (tagPath[pos][1] == null || tagPath[pos][1].equals(""))
				return node;
			if (attr != null) {
				Node id = attr.getNamedItem(tagPath[pos][1]);
				if (id != null) {
					if (tagPath[pos][2].equals(id.getNodeValue())) {
						return node;
					}
				}
			}
		}
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return null;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			NamedNodeMap attr = _node.getAttributes();
			if (tagPath[pos][0].equals(_node.getNodeName()) == false)
				continue;
			if (tagPath[pos][1] == null || tagPath[pos][1].equals(""))
				return _node;
			if (attr != null) {
				Node id = attr.getNamedItem(tagPath[pos][1]);
				if (id != null) {
					if (tagPath[pos][2].equals(id.getNodeValue())) {
						return _node;
					}
				}
			}
		}
		for (int i = 0; i < childLen; i++) {
			Node _node = getNodeByTagPath(currentChildren.item(i), tagPath, pos);
			if (_node != null)
				return _node;
		}
		return null;
	}

	public Node getDomNode(byte[] cnt) throws Exception {
		return getDomNode(cnt, "utf-8");
	}

	public Node getDomNode(byte[] cnt, String charEncoding) throws Exception {
		try {
			InputSource input = new InputSource(new ByteArrayInputStream(cnt));
			input.setEncoding(charEncoding);
			return parse(input);
		} catch (Exception e) {
			LOG.error("Failed with the following Exception: ", e);
			throw new IOException(e);
		}
	}

	public String getNodeValue(Node node) {
		StringBuilder sb = new StringBuilder();
		getTextHelper(sb, node);
		return sb.toString().replaceAll("<P></P>", "");
	}

	public String getNodeAttrValue(Node node, String attrName) {
		NamedNodeMap attr = node.getAttributes();
		if (attr != null) {
			Node attrNode = attr.getNamedItem(attrName);
			if (attrNode != null) {
				return attrNode.getNodeValue();
			}
		}
		return null;
	}

}
