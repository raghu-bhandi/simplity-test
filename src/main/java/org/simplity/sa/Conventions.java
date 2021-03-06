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

package org.simplity.sa;

/**
 * set-up parameters expected in the servlet context.
 *
 * @author simplity.org
 *
 */
public class Conventions {
	/**
	 * resource names can use either resource or file format. For example a
	 * json resource could be set to "c:/a/b/c.json" or "res/c.json"
	 *
	 * Each resource has a default value that is used in case this parameter is
	 * not available in the context. This is documented with the constant with
	 * _DEFAULT suffix
	 *
	 * @author simplity.org
	 *
	 */
	public static class Resource {
		/**
		 * root path for which the HttpAgent servlet is mapped to. This depends
		 * on deployment.for example /api/
		 */
		public static final String REST_ROOT = "sa_restRoot";
		/**
		 * resource name for path-mapping for httpAgent.
		 */
		public static final String REST_PATHS_DEFAULT = "res/";

		/**
		 * root where you can find application.xml configuration file for a
		 * simplity app. NOTE: Simplity has an underlying design that require it
		 * to read all files in a folder, because of which deploying resource as
		 * part of jar does not work. this path should either be a folder path
		 * (like c:/...) or the resources should be deployed inside class folder
		 * of WEB-INF
		 */
		public static final String SIMPLITY_ROOT = "sa_simplityRoot";
		/**
		 * default for SIMPLITY_ROOT
		 */
		public static final String SIMPLITY_ROOT_DEFAULT = "res/";

		/**
		 * rest path mappings are expected in this file/resource under
		 * component-root.
		 */
		public static final String REST_PATHS = "restPaths.json";

		/**
		 * application configuration file name
		 */
		public static final String APPLICATION_CONFIG = "application.xml";

		/**
		 * service name mapping between name used by client and server
		 */
		public static final String SERVICE_ALIASES = "serviceAliases.json";

		private Resource() {
			// forbidden
		}

	}

	/**
	 * general conventions within the programming paradigm. Not used by set-up
	 * or admin person
	 *
	 * @author simplity.org
	 *
	 */
	public static class General {
		/**
		 * field names concatenated with this separator
		 */
		public static final String FIELD_NAME_SEPARATOR = ",";

		private General() {
			// forbidden
		}
	}

	/**
	 * field names used in the context or data carriers. all names are prefixed
	 * with _sa to avoid clash with other run-time names
	 *
	 * @author simplity.org
	 *
	 */
	public static class FieldNames {
		/**
		 * field name that has the operation. maps to HTTP METHOD. used by
		 * requestUsed by ServiceRequest
		 */
		public static final String REQUEST_OPERATION = "sa_operation";
		/**
		 * field name for service name. Could be in header. used by
		 * ServiceRequest.
		 */
		public static final String SERVICE_NAME = "sa_serviceName";
		/**
		 * header field used to indicate that this request is for testing, and
		 * the
		 * response should be for this test id. Honored by server if the server
		 * is
		 * in stub-mode for simulation
		 */
		public static final String TEST_ID = "sa_testId";
	}

	/**
	 * tag and attribute names in json/xml resources
	 *
	 * @author simplity.org
	 *
	 */
	public static class TagNames {
		/**
		 * attribute name of payload. If this is not found, then the whole
		 * object is
		 * considered to be payload
		 */
		public static final String PAYLOAD = "payload";
		/**
		 * attribute name for messages
		 */
		public static final String MESSAGES = "messages";
		/**
		 * tag name for each message
		 */
		public static final String MESSAGE = "message";

		/**
		 * attribute name for session fields. this is an array of Message
		 * objects
		 */
		public static final String SESSION_FIELDS = "sessionFields";
		/**
		 * tag name for each field in sessionFields
		 */
		public static final String SESSION_FIELD = "field";
		/**
		 * attribute name of field name. required for xml, not for json
		 */
		public static final String FIELD_NAME = "name";

		/**
		 * attribute name of field value. required for xml, not for json.
		 */
		public static final String FIELD_VALUE = "value";

		/**
		 * attribute name of result. default is ALL_OK
		 */
		public static final String RESULT = "result";
		/**
		 * if all paths have a common base/prefix, use this tag to specify the
		 * common part, and specify individual paths without this
		 */
		public static final String BASE_PATH = "basePath";
		/**
		 * collection of path-service mappings
		 */
		public static final String PATHS = "paths";

		/**
		 * service names may be qualified with module etc as prefix. Use this as
		 * common prefix for all service names in this resource
		 */
		public static final String SERVICE_NAME_PREFIX = "serviceNamePrefix";

		/**
		 * list of services. If SERVICE_NAME_PREFIX is used, each attribute name
		 * in this list is prefixed to get the fully-qualified name
		 */
		public static final String SERVICES = "services";

	}

	/**
	 * http set-up. web.xml is quite convenient. however, a resource bundle can
	 * be used with another startup servlet to load them into servlet context
	 *
	 * @author simplity.org
	 *
	 */
	public static class Http {
		/**
		 * name of cookies that need to be extracted as data
		 */
		public static final String COOKIES = "sa_cookies";
		/**
		 * header field names to be extracted as data, other than serviceName
		 */
		public static final String HEADERS = "sa_headers";
		/**
		 * session field names that are to be extracted as data
		 */
		public static final String SESSION_FIELDS = "sa_sessionFields";

		/**
		 * any object to be used from request as getAttributes
		 */
		public static final String REQUEST_ATTRIBUTES = "sa_requestAttributes";

		private Http() {
			// forbidden
		}
	}

	/*
	 * not used, but may require if/when we introduce data packets
	 */
	/**
	 * name of the field for error message in data-packet
	 */
	public static final String ERROR_MESSAGE_FIELD_NAME = "sa_errorMessage";
	/**
	 * name of the field for error message in data-packet
	 */
	public static final String PAYLOAD_FIELD_NAME = "sa_payload";
	/**
	 * name of the field for error message in data-packet
	 */
	public static final String PAYLOAD_TYPE_FIELD_NAME = "sa_payloadType";
	/**
	 * pay load type is xml
	 */
	public static final String PAYLOAD_XML = "xml";
	/**
	 * pay load type is json
	 */
	public static final String PAYLOAD_JSON = "json";
}