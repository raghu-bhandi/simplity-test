/*
 * Copyright (c) 2018 simplity.org
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.simplity.http;

import java.util.HashMap;
import java.util.Map;

import org.simplity.json.JSONObject;
import org.simplity.sa.Conventions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tree representation of all valid pats for which service names are mapped
 *
 * @author simplity.org
 *
 */
public class Paths {
	static final Logger logger = LoggerFactory.getLogger(Paths.class);
	static final String PATH_SEP_STR = "/";
	/*
	 * root node. PathNode has the tree structure built into that.
	 */
	private Node rootNode;

	/**
	 * create an empty tree to which paths can be added later
	 */
	public Paths() {
		this.rootNode = new Node(null, false);
	}

	/**
	 * add a path-service mapping
	 *
	 * @param path
	 *            example a/{invoice}/{operation}
	 * @param services
	 *            each element is a method whose value is service. In case there
	 *            is a single service irrespective of method, then a single
	 *            member with "" is to be used
	 *            example {"post":"updateInvoice", "get":"invoiceDetails",,,,}
	 * @param servicePrefix
	 *            if non-null, every service name is prefixed with this to get
	 *            its fully-qualified name
	 * @return true if the mapping got added. False in case this path is a
	 *         duplicate, and hence ignored
	 */
	public boolean addPath(String path, JSONObject services, String servicePrefix) {
		String[] parts = path.split(PATH_SEP_STR);
		Node node = this.rootNode;
		for (String part : parts) {
			part = part.trim();
			if (part.isEmpty()) {
				logger.info("empty part ignored");
				continue;
			}
			if (part.charAt(0) == '{') {
				/*
				 * we assume that there are no syntax errors. so this token is
				 * {fieldName}
				 */
				String fieldName = part.substring(1, part.length() - 1);
				node = node.setFieldChild(fieldName);
			} else {
				node = node.setPathChild(part);
			}
			if (node == null) {
				logger.error("child node was not created. Giving up on adding services");
				return false;
			}
		}
		/*
		 * attach services for this node
		 */
		return node.setServices(services, servicePrefix);
	}

	/**
	 * add all paths from a json object. Each element of this object is a
	 * path-services mapping.
	 * e.g. {"/a/{b}/c": {"post":"addInvoice","get":"invoiceDetails",,,},
	 * "/a/{b}/d" : {},}
	 *
	 * @param json
	 *            josn object as read from the standard json file for paths.
	 *            non-null.
	 */
	public void addPaths(JSONObject json) {
		String pathPrefix = json.optString(Conventions.TagNames.BASE_PATH);
		String servicePrefix = json.optString(Conventions.TagNames.SERVICE_NAME_PREFIX);
		JSONObject paths = json.optJSONObject(Conventions.TagNames.PATHS);
		if (paths == null) {
			if (pathPrefix == null && servicePrefix == null) {
				/*
				 * possible that base is not used, and file contains only paths
				 */
				paths = json;
			} else {
				logger.warn("No paths found in json");
				return;
			}
		}

		int nbr = 0;
		int nok = 0;
		for (String key : paths.keySet()) {
			String aPath = key;
			if(pathPrefix != null) {
				aPath = pathPrefix + aPath;
			}
			boolean ok = this.addPath(aPath, paths.optJSONObject(key), servicePrefix);
			if (ok) {
				nbr++;
			} else {
				nok++;
				logger.error(
						"Path {} is either duplicate or can not be uniquely resolved due to over-lapping part with field of another path",
						key);
			}
		}
		if (nok > 0) {
			logger.error("{} paths not added.", nok);
		}
		if (nbr > 0) {
			logger.info("{} paths added", nbr);
		} else {
			logger.error("No paths added.");
		}
	}

	/**
	 * parse a path received from client and return the corresponding service.
	 * Also, extract any path-fields into the collection
	 *
	 * @param path
	 *            requested pat from client e.g. /a/123/add/
	 * @param method
	 *            http method
	 * @param fields
	 *            to which path-fields are to be extracted to. null if we need
	 *            not do that.
	 * @return service to which this path is mapped to. null if no service is
	 *         mapped
	 */
	public String parse(String path, String method, Map<String, Object> fields) {
		if (path == null || path.isEmpty()) {
			return null;
		}
		Node node = this.findNodeForPath(path, fields);
		if (node == null) {
			logger.info("{} is an invalid path", path);
			return null;
		}
		/*
		 * get the spec at this node. In case we do not have one at this node,
		 * then we keep going up
		 *
		 */
		while (node != null) {
			if (node.isValidEndNode()) {
				return node.getService(method);
			}
			/*
			 * it is possible that this part is a field and is optional..
			 */
			if (node.isFieldChild()) {
				node = node.getParent();
			}else {
				break;
			}
		}
		/*
		 * So, the path was partial part of a valid path
		 */
		logger.info("{} is an incomplete path", path);
		return null;
	}

	/**
	 * find the node corresponding to the path
	 *
	 * @param path
	 *            non-null, non-empty
	 * @param fields
	 * @return
	 */
	private Node findNodeForPath(String path, Map<String, Object> fields) {
		Node node = this.rootNode;
		if (node.isLeaf()) {
			logger.info("We have an empty list of paths!!");
			return null;
		}
		/*
		 * go down the path as much as we can.
		 */
		String[] parts = path.split(PATH_SEP_STR);
		for (String part : parts) {
			if (part.isEmpty()) {
				continue;
			}
			Node child = node.getPathChild(part);
			if (child != null) {
				/*
				 * keep going down this path
				 */
				node = child;
				continue;
			}
			/*
			 * this is not path. Is it field?
			 */
			child = node.getFieldChild();
			if (child == null) {
				/*
				 * this is not a valid path
				 */
				logger.warn("Path {} is invalid starting at token {}", path, part);
				return null;
			}
			/*
			 * copy this part as field value
			 */
			if (fields != null) {
				fields.put(node.getFieldName(), part);
			}
			node = child;
		}
		return node;
	}
}

/**
 * this class is exclusively used by PathTree, but is not an inner class. Hence
 * we have put this inside this compilation unit
 *
 * @author simplity.org
 *
 */
class Node {
	private static final String DUPLICATE = "Duplicate path-service mapping detected and ignored";
	/**
	 * non-null only if this this node is for a field
	 */
	private String fieldName = null;
	/**
	 * non-null when fieldName is non-null. This is the sole child from this
	 * node irrespective of the
	 * field value
	 */
	private Node fieldChild = null;
	/**
	 * child paths from here. one entry for each possible value of the path part
	 * from here. null if
	 * this is a field
	 */
	private Map<String, Node> children;
	/**
	 * default or only service that is mapped to this path Null if no services
	 * is mapped at this level.
	 */
	private String defaultService;
	/**
	 * non-null if more than one service is mapped, based on method
	 */
	private Map<String, String> services;

	/**
	 * way to go up the path. required when when spec may be assigned at a
	 * sub-path and not at full
	 * path.
	 */
	private final Node parent;

	private final boolean isFieldChild;

	/**
	 * construct a Path node under the parent
	 *
	 * @param parent
	 */
	Node(Node parent, boolean isField) {
		this.parent = parent;
		this.isFieldChild = isField;
	}

	/**
	 * @return true if this is a leaf node. False if this node has at least one
	 *         child node, either a field-child or path-child
	 */
	public boolean isLeaf() {
		return this.children == null && this.fieldChild == null;
	}

	/**
	 * @return true if this node is the field-child of its parent. false if it
	 *         is a path-child
	 */
	public boolean isFieldChild() {
		return this.isFieldChild;
	}

	/**
	 * @return true if this path is a valid complete path. It means that service
	 *         names are mapped to this path
	 */
	boolean isValidEndNode() {
		return this.defaultService != null || this.services != null;
	}

	/**
	 * @param fieldName
	 *            the fieldName to set
	 * @return child-node associated with this field
	 */
	Node setFieldChild(String field) {
		if (this.fieldName == null) {
			this.fieldName = field;
			this.fieldChild = new Node(this, true);
		} else {
			if (field.equals(this.fieldName)) {
				Paths.logger.error("two paths canot have different field names at same part of path.");
				return null;
			}
		}
		return this.fieldChild;
	}

	/**
	 * @return the fieldName
	 */
	String getFieldName() {
		return this.fieldName;
	}

	/**
	 * set a child path for this path-part
	 *
	 * @param pathPart
	 *            a part of the path
	 * @return child node for this path-part
	 */
	Node setPathChild(String pathPart) {
		Node child = null;
		if (this.children != null) {
			child = this.children.get(pathPart);
			if (child != null) {
				return child;
			}
		} else {
			this.children = new HashMap<String, Node>();
		}
		child = new Node(this, false);
		this.children.put(pathPart, child);
		return child;
	}

	/**
	 * @param pathPart
	 *            as received from client. can be value of field in case this
	 *            node is for a field
	 * @return child node for this pathPart. null if no child node for this
	 *         part.
	 */
	Node getPathChild(String pathPart) {
		if (this.children == null) {
			return null;
		}
		return this.children.get(pathPart);
	}

	/**
	 * @return child associated with the field
	 */
	Node getFieldChild() {
		return this.fieldChild;
	}

	/**
	 * set service associated with methods
	 *
	 * @param services
	 *
	 * @param servicePrefix
	 *            if non-null, this is prefixed for each service name
	 */
	boolean setServices(JSONObject services, String servicePrefix) {
		if (this.defaultService != null || this.services != null) {
			Paths.logger.error(DUPLICATE);
			return false;
		}
		/*
		 * is there mapping by method?
		 */
		String service = services.optString("");
		if (servicePrefix != null) {
			service = servicePrefix + service;
		}
		if (service != null) {
			this.defaultService = service;
			return true;
		}
		this.services = new HashMap<String, String>();
		for (String method : services.keySet()) {
			service = services.optString(method);
			if (servicePrefix != null) {
				service = servicePrefix + service;
			}
			this.services.put(method.toLowerCase(), service);
		}
		return true;
	}

	/**
	 * map a service to method
	 *
	 * @param method
	 *            empty or null if method is to be ignored and same service for
	 *            all methods
	 * @param service
	 */
	void setService(String method, String service) {
		if (method == null || method.isEmpty()) {
			if (this.defaultService != null || this.services != null) {
				Paths.logger.error(DUPLICATE);
				return;
			}
			this.defaultService = service;

			return;
		}
		if (this.defaultService != null) {
			Paths.logger.error("default service exists, and hence service {} is not added for method {}", service,
					method);
			return;
		}
		String lowerCaseMethod = method.toLowerCase();
		if (this.services == null) {
			this.services = new HashMap<String, String>();
		} else if (this.services.containsKey(lowerCaseMethod)) {
			Paths.logger.error(DUPLICATE);
			return;
		}
		this.services.put(lowerCaseMethod, service);
	}

	/**
	 * @param method
	 * @return service spec associated with this method, or null if no such spec
	 */
	String getService(String method) {
		if (this.defaultService != null) {
			return this.defaultService;
		}
		if (method == null) {
			Paths.logger
					.info("No method specified, but this path has no default service. Service name is not determined.");
			return null;
		}
		if (this.services == null) {
			Paths.logger.info("No service attached to this path.");
			return null;
		}
		String serviceName = this.services.get(method.toLowerCase());
		if (serviceName == null) {
			Paths.logger.info("No service attached to method {}", method);
			return null;
		}
		return serviceName;
	}

	/**
	 * @return the parent node. null for the root node
	 */
	Node getParent() {
		return this.parent;
	}
}
