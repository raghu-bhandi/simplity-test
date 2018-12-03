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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.simplity.json.JSONObject;
import org.simplity.kernel.Application;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.util.IoUtil;
import org.simplity.sa.Conventions;
import org.simplity.sa.PayloadType;
import org.simplity.sa.ServiceAgent;
import org.simplity.sa.ServiceRequest;
import org.simplity.sa.ServiceResponse;
import org.simplity.sa.ServiceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent (or controller) that receives any requests for a service over HTTP.
 * Each
 * application should have a concrete class that extends this class with app
 * specific code put into the abstract methods
 *
 *
 * This has the init() to bootstrap Simplity application as well.
 *
 * @author simplity.org
 *
 */
public abstract class HttpClient extends HttpServlet {
	private static final long serialVersionUID = 1L;
	protected static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

	private static final String UTF = "UTF-8";
	private static final String XML_CONTENT = "application/xml";
	private static final String JSON_CONTENT = "application/json";
	/**
	 * path-to-service mappings
	 */
	protected Paths mappedPaths;
	/**
	 * in case we are to extract some data from cookies,header and session..
	 */
	protected Set<String> cookieFields;
	/**
	 * standard header fields to be extracted
	 */
	protected String[] headerFields = null;
	/**
	 * standard session fields to be extracted
	 */
	protected String[] sessionFields = null;

	/**
	 * objects from requestAttributes to be extracted
	 */
	protected String[] requestAttributes = null;

	/**
	 * root path that is mapped for this REST module. e.g /api/ for the url
	 * http://a.b.c/site/api/customer/{custId}
	 */
	protected String rootFolder = null;

	/**
	 * map of service names used by client to the one used by server.
	 */
	protected JSONObject serviceAliases = null;
	/**
	 * legth of root folder is all that we use during execution. keeping it
	 * rather than keep checking it.
	 */
	private int rootFolderLength = 0;

	/**
	 * a service agent is connected at run time based on set-up. null at run
	 * time indicates set-up failure
	 */
	protected ServiceAgent agent;

	/**
	 * use streaming payload if the service layer is inside the same JVM as
	 * this. if this is set to false, request payload is read into object before
	 * passing to teh service layer. Similarly, service layer passes the
	 * response object back which is written out to response stream
	 */
	protected boolean useStreamingPayload = true;

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.GenericServlet#init()
	 */
	@Override
	public void init() throws ServletException {
		super.init();
		ServletContext ctx = this.getServletContext();
		this.rootFolder = ctx.getInitParameter(Conventions.Resource.REST_ROOT);
		if (this.rootFolder != null) {
			this.rootFolderLength = this.rootFolder.length();
		}

		this.bootstrap(ctx);
		this.loadPaths();
		this.loadServiceAliases();
		this.setHttpParams(ctx);
		this.appSpecificInit(ctx);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doDelete(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doHead(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doOptions(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doOptions(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see javax.servlet.http.HttpServlet#doTrace(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.serve(req, resp);
	}

	/**
	 * serve an in-bound request.
	 *
	 * @param req
	 *            http request
	 * @param resp
	 *            http response
	 * @throws IOException
	 *             IO exception
	 *
	 */
	public void serve(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		String serviceName = null;
		long bigin = System.currentTimeMillis();

		try (Reader reqReader = req.getReader(); Writer respWriter = new PrintWriter(resp.getOutputStream())) {

			String ct = req.getContentType();
			boolean isJson = ct == null || ct.indexOf("xml") == -1;
			PayloadType pt = null;

			if (isJson) {
				resp.setContentType(JSON_CONTENT);
				pt = this.useStreamingPayload ? PayloadType.JSON_STREAM : PayloadType.JSON_TEXT;
			} else {
				resp.setContentType(XML_CONTENT);
				pt = this.useStreamingPayload ? PayloadType.XML_STREAM : PayloadType.XML_TEXT;
			}

			this.setResponseHeaders(resp);

			if (this.agent == null) {
				String msg = "Server app is not set-up properly for a Service Agent. All requests are responded back as internal error";
				logger.error(msg);
				this.respondWithError(resp, msg, respWriter);
				return;
			}

			try {
				/*
				 * data from non-payload sources, like header and cookies is in
				 * this map
				 */
				Map<String, Object> fields = new HashMap<>();
				/*
				 * path-data is extracted during path-parsing for serviceNAme
				 */
				serviceName = this.getServiceName(req, fields);
				if (serviceName == null) {
					logger.warn("No service name is inferred from request.");
					this.respondWithError(resp, "Sorry, that request is beyond us!!", respWriter);
					return;
				}

				/*
				 * get all non-payload data
				 */
				this.mineFields(req, fields);

				ServiceRequest request = null;
				ServiceResponse response = null;
				if (this.useStreamingPayload) {
					request = new ServiceRequest(serviceName, pt, reqReader);
					response = new ServiceResponse(respWriter, isJson);
				} else {
					String json = IoUtil.readerToText(reqReader);
					request = new ServiceRequest(serviceName, pt, json);
					response = new ServiceResponse(pt);
				}

				/*
				 * app specific code to copy anything from client-layer to
				 * request as well as set anything to response
				 */
				boolean okToProceed = this.prepareRequestAndResponse(serviceName, req, resp, request, response, fields);

				if (okToProceed) {
					this.agent.serve(request, response);
					/*
					 * app-specific hook to do anything before responding back
					 */
					this.postProcess(req, resp, request, response);
				}

				ServiceResult result = response.getServiceResult();
				FormattedMessage[] messages = response.getMessages();
				/*
				 * how are messages sent back to client? TODO:
				 */
				if (messages.length > 0) {
					logger.info("We want to send following messages to the client");
					for (FormattedMessage msg : messages) {
						logger.info("Message Type = {}, text={}", msg.messageType, msg.text);
					}
				}
				logger.info("Service {} ended with result={} and {} message/s", serviceName, result, messages.length);

				if (result == ServiceResult.ALL_OK) {
					if (this.useStreamingPayload == false) {
						respWriter.write(response.getPayloadText());
					}
					logger.info("Service {} claimed {} ms as its execution time", serviceName,
							response.getExecutionTime());
				} else {
					/*
					 * TODO: device a way to respond back with an error.
					 */
					this.respondWithError(resp, "Sorry, your request failed to execute : " + result, respWriter);
				}
			} catch (Exception e) {
				String msg = "Error occured while serving the request";
				logger.error(msg, e);
				this.respondWithError(resp, msg, respWriter);
			}
		} finally {
			if (serviceName == null) {
				serviceName = "unknown";
			}
			logger.info("Http server took {} ms to deliver service {}", System.currentTimeMillis() - bigin,
					serviceName);
		}
	}

	/**
	 * @param resp
	 */
	private void setResponseHeaders(HttpServletResponse resp) {
		resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
		resp.setDateHeader("Expires", 0);
	}

	/**
	 * @param resp
	 * @param outStream
	 * @param string
	 * @throws IOException
	 */
	private void respondWithError(HttpServletResponse resp, String message, Writer writer) throws IOException {
		resp.setStatus(500);
		writer.write(message);
	}

	/**
	 * clients may send data via query-string, cookies and header fields
	 *
	 * @param req
	 * @param fields
	 */
	private void mineFields(HttpServletRequest req, Map<String, Object> fields) {
		String qry = req.getQueryString();
		if (qry != null) {
			for (String part : qry.split("&")) {
				String[] pair = part.split("=");
				String val;
				if (pair.length == 1) {
					val = "";
				} else {
					val = this.decode(pair[1]);
				}
				fields.put(pair[0], val);
			}
		}

		if (this.cookieFields != null) {
			Cookie[] cookies = req.getCookies();
			if (cookies != null) {
				for (Cookie cookie : cookies) {
					String name = cookie.getName();
					if (this.cookieFields.contains(name)) {
						fields.put(name, cookie.getValue());
					}
				}
			}
		}

		if (this.headerFields != null) {
			for (String name : this.headerFields) {
				String value = req.getHeader(name);
				if (value != null) {
					fields.put(name, value);
				}
			}
		}

		if (this.sessionFields != null) {
			ServletContext ctx = this.getServletContext();
			for (String name : this.sessionFields) {
				Object value = ctx.getAttribute(name);
				if (value != null) {
					fields.put(name, value);
				}
			}
		}

		if (this.requestAttributes != null) {
			for (String name : this.requestAttributes) {
				Object value = req.getAttribute(name);
				if (value != null) {
					fields.put(name, value);
				}
			}
		}
	}

	private String decode(String text) {
		try {
			return URLDecoder.decode(text, UTF);
		} catch (UnsupportedEncodingException e) {
			logger.error("How come {} is not a valid encoding?", UTF);
			/*
			 * we do know that this is supported. so, this is unreachable
			 * code.
			 */
			return text;
		}
	}

	private String getServiceName(HttpServletRequest req, Map<String, Object> fields) {
		String serviceName = req.getHeader(Conventions.FieldNames.SERVICE_NAME);
		if (serviceName == null) {
			serviceName = (String) fields.get(Conventions.FieldNames.SERVICE_NAME);
		}
		if (serviceName != null) {
			logger.info("Service name = {} extracted from header/query", serviceName);
			if (this.serviceAliases != null) {
				Object alias = this.serviceAliases.opt(serviceName);
				if (alias != null) {
					logger.info("client requested service name {} is mapped {} ", serviceName, alias);
					serviceName = alias.toString();
				}
			}
			return serviceName;
		}

		if (this.mappedPaths == null) {
			logger.info("Request header has no service name, and we are not set-up to translate path to serviceName");
			return null;
		}

		String uri = this.decode(req.getRequestURI());
		/*
		 * assuming http://www.simplity.org:8020/app1/subapp/a/b/c?a=b&c=d
		 * path now is set to /app1/subapp/a/b/c
		 * we need to get a/b/c as RESTful path
		 */

		int idx = req.getContextPath().length() + this.rootFolderLength;
		String path = uri.substring(idx);
		logger.info("Going to use path={} from uri={}", path, uri);
		serviceName = this.mappedPaths.parse(path, req.getMethod(), fields);
		logger.info("Request is mapped to service {} ", serviceName);
		return serviceName;
	}

	private void loadPaths() {
		String resName = ComponentType.getComponentRoot() + Conventions.Resource.REST_PATHS;
		String text = IoUtil.readResource(resName);
		if (text == null) {
			logger.error("Resource {} could not be read. Paths will not be mapped to service names for REST calls.",
					resName);
			return;
		}
		this.mappedPaths = new Paths();
		this.mappedPaths.addPaths(new JSONObject(text));
	}

	private void loadServiceAliases() {
		String resName = ComponentType.getComponentRoot() + Conventions.Resource.SERVICE_ALIASES;
		String text = IoUtil.readResource(resName);
		if (text == null) {
			logger.warn("Service aliases resource {} could not be read. No service alaises set for this app.", resName);
			return;
		}

		try {
			JSONObject json = new JSONObject(text);
			this.serviceAliases = json;
			logger.info("{} service aliases loaded.", json.length());
		} catch (Exception e) {
			logger.error("Contents of resource {} is not a valid json. Error : {}", resName, e.getMessage());
		}
	}

	/**
	 * @param ctx
	 */
	private void bootstrap(ServletContext ctx) {
		String text = ctx.getInitParameter(Conventions.Resource.SIMPLITY_ROOT);
		if (text == null) {
			logger.warn("{} is not set to identify root folder for components. Default vale of {} is tried.",
					Conventions.Resource.SIMPLITY_ROOT, Conventions.Resource.SIMPLITY_ROOT_DEFAULT);
			text = Conventions.Resource.SIMPLITY_ROOT_DEFAULT;
		}
		try {
			Application.bootStrap(text);
			/*
			 * if bootstrapping is successful, it would have created an
			 * agent. else following call would throw an exception
			 */
			this.agent = ServiceAgent.getAgent();
		} catch (Exception e) {
			logger.error("Error while bootstrapping using root folder {}. Error : {}", text, e.getMessage());
		}

		/*
		 * service agent is
		 */
		if (this.agent == null) {
			logger.error("Service agent is not available due to set-up isssues");
		}
	}

	/**
	 * @param ctx
	 */
	private void setHttpParams(ServletContext ctx) {
		String text = ctx.getInitParameter(Conventions.Http.COOKIES);
		if (text == null) {
			logger.info("{} is not set. No cookie will be used.", Conventions.Http.COOKIES);
		} else {
			logger.info("{} is used as set of cookie name/s to be extracted as fields for each request.", text);
			this.cookieFields = new HashSet<>();
			for (String fieldName : text.split(Conventions.General.FIELD_NAME_SEPARATOR)) {
				this.cookieFields.add(fieldName);
			}
		}

		text = ctx.getInitParameter(Conventions.Http.HEADERS);
		if (text == null) {
			logger.info("{} is not set. No header fields will be used as data.", Conventions.Http.HEADERS);
		} else {
			logger.info("{} is/are the header fields to be extracted as data.", text);
			this.headerFields = text.split(Conventions.General.FIELD_NAME_SEPARATOR);
		}

		text = ctx.getInitParameter(Conventions.Http.SESSION_FIELDS);
		if (text == null) {
			logger.info("{} is not set. No session fields will be used as data.", Conventions.Http.SESSION_FIELDS);
		} else {
			logger.info("{} is/are the session fields to be extracted as data.", text);
			this.sessionFields = text.split(Conventions.General.FIELD_NAME_SEPARATOR);
		}

		text = ctx.getInitParameter(Conventions.Http.REQUEST_ATTRIBUTES);
		if (text == null) {
			logger.info("{} is not set. No attributes from HttpSevletRequest will be used as data.",
					Conventions.Http.REQUEST_ATTRIBUTES);
		} else {
			logger.info("{} is/are the request attributes that will be used as input data", text);
			this.requestAttributes = text.split(Conventions.General.FIELD_NAME_SEPARATOR);
		}
	}

	/**
	 * Called before requesting for this service from server agent. Typically,
	 * appUser and fields are set. clientContext may also be set. Refer to
	 * <code>ExampleClient</code>
	 *
	 * @param req
	 *            service request
	 * @param resp
	 *            service response
	 * @param request
	 *            http request
	 * @param response
	 *            http response
	 * @param fields
	 *            fields picked-up from all request/session as per standard
	 * @return true if all ok, nad service should be called. false in case some
	 *         error is detected.
	 */
	protected abstract boolean prepareRequestAndResponse(String serviceName, HttpServletRequest req,
			HttpServletResponse resp, ServiceRequest request, ServiceResponse response, Map<String, Object> fields);

	/**
	 * application specific functionality in web-layer after service layer
	 * returns with success.
	 *
	 * @param httpRequest
	 * @param httpResponse
	 * @param serviceRequest
	 * @param serviceResponse
	 */
	protected abstract void postProcess(HttpServletRequest httpRequest, HttpServletResponse httpResponse,
			ServiceRequest serviceRequest, ServiceResponse serviceResponse);

	/**
	 * app specific aspects to be handled at init() time
	 *
	 * @param ctx
	 */
	protected abstract void appSpecificInit(ServletContext ctx);
}
