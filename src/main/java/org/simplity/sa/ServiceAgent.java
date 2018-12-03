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

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.simplity.json.JSONObject;
import org.simplity.kernel.Application;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.Messages;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.expr.Expression;
import org.simplity.kernel.util.IoUtil;
import org.simplity.kernel.util.JsonUtil;
import org.simplity.kernel.value.Value;
import org.simplity.service.AccessControllerInterface;
import org.simplity.service.OutputData;
import org.simplity.service.ServiceCacherInterface;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceProtocol;
import org.simplity.tp.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent serving end-clients. End-clients think this agent is THE SERVER and
 * expect her to deliver any service. She in turn uses a network of
 * module agents to deliver the service.
 *
 * @author simplity.org
 *
 */
public class ServiceAgent {
	private static final Logger logger = LoggerFactory.getLogger(ServiceAgent.class);
	private static Value DUMMY_USER = null;

	/**
	 * singleton instance that is instantiated with right parameters
	 */
	private static ServiceAgent instance = null;

	/**
	 * set up agent with required input. This is to be executed before asking
	 * for an instance
	 *
	 * @param userIdIsNumber
	 *            user id may be used as a field in tables for audit purposes.
	 *            This is handled automatically with the right set-up. user id
	 *            can be number or text. false is used as default
	 * @param cacheManager
	 *            instance that handles caching of responses from service. null
	 *            if caching is not enabled.
	 * @param securityGuard
	 *            instance that checks whether given logged-in user has access
	 * @param prePostProcessor app specific hook for carrying out some common tasks during the service invocation life cycle
	 * @param simulateWithData
	 *            true in case you are running the services in a dummy mode
	 *            where
	 *            execution is skipped, and local data is used instead. Should
	 *            be false in production
	 */
	public static void setUp(boolean userIdIsNumber, ServiceCacherInterface cacheManager, AccessControllerInterface securityGuard,
			ServicePrePostProcessorInterface prePostProcessor,  boolean simulateWithData) {

		logger.info("Service Agent is being set-up......");
		instance = new ServiceAgent(userIdIsNumber, cacheManager, securityGuard, prePostProcessor, simulateWithData);
		if (userIdIsNumber) {
			DUMMY_USER = Value.newIntegerValue(100);
		} else {
			DUMMY_USER = Value.newTextValue("100");
		}
		if (simulateWithData) {
			logger.info(
					"Service Agent is set up to use local data to simulate service execution. Service actions will be ignored");
		}

	}

	/**
	 * to be called ONLY after bootstrapping taken care of. That is, after a
	 * call to setUp()
	 *
	 * @return an instance for use. null if app is not set-up yet, or set-up
	 *         failed
	 */
	public static ServiceAgent getAgent() {
		if (instance == null) {
			throw new ApplicationError("Service Agent is not set up, but there are requests for service!!");
		}
		return instance;
	}

	/**
	 * is user Id a numeric value? default is text
	 */
	private final boolean numericUserId;

	/**
	 * service response may be cached. This may also be used to have fake
	 * responses during development
	 */
	private final ServiceCacherInterface cacheManager;
	/**
	 * registered access control class
	 */
	private final AccessControllerInterface securityManager;

	/**
	 * during development/testing, we may skip executing a service, and use
	 * local data instead
	 */
	private final boolean useLocalData;

	/**
	 * hook used by app during service invocation life cycle
	 */
	private ServicePrePostProcessorInterface applicationHook;

	/**
	 * * We create an immutable instance fully equipped with all plug-ins
	 *
	 */
	private ServiceAgent(boolean userIdIsNumber, ServiceCacherInterface cacher, AccessControllerInterface guard, ServicePrePostProcessorInterface prePostProcessor,
			boolean useLocalData) {
		this.numericUserId = userIdIsNumber;
		this.cacheManager = cacher;
		this.securityManager = guard;
		this.useLocalData = useLocalData;
		this.applicationHook = prePostProcessor;
	}

	/**
	 * main RPC into the server
	 *
	 * @param request
	 * @param response
	 */
	public void serve(ServiceRequest request, ServiceResponse response) {

		String serviceName = request.getServiceName();
		Service service = ComponentManager.getServiceOrNull(serviceName);
		if (service == null) {
			logger.error("Service {} is not served on this server", serviceName);
			response.setResult(ServiceResult.NO_SUCH_SERVICE);
			return;
		}

		/*
		 * is it accessible to user?
		 */
		if (this.securityManager != null && this.securityManager.okToServe(service, request) == false) {
			logger.error("Logged in user is not authorized for Service {} ", serviceName);
			response.setResult(ServiceResult.INSUFFICIENT_PRIVILEGE);
			return;
		}

		long bigin = System.currentTimeMillis();
		AppUser user = request.getUser();
		if (user == null) {
			logger.info("Service requested with no user. Dummy user is assumed.");
			user = new AppUser(DUMMY_USER);
		}
		ServiceContext ctx = new ServiceContext(serviceName, user);

		this.callService(ctx, request, response, service);
		response.setExecutionTime((int) (System.currentTimeMillis() - bigin));
		List<FormattedMessage> messages = ctx.getMessages();
		if (messages != null && messages.size() > 0) {
			response.setMessages(messages.toArray(new FormattedMessage[0]));
		}
		if (ctx.isInError()) {
			response.setResult(ServiceResult.INVALID_DATA);
		} else {
			response.setResult(ServiceResult.ALL_OK);
		}
		return;
	}

	private void callService(ServiceContext ctx, ServiceRequest request, ServiceResponse response, Service service) {

		try {
			if(this.applicationHook != null) {
				if(this.applicationHook.beforeInput(request, response, ctx) == false) {
					logger.info("App specific hook requested that the service be abandoned before iinputting data.");
					return;
				}
			}
			request.copyToServiceContext(ctx, service);
			if (ctx.isInError()) {
				logger.info("Input data had errors. Service not invoked.");
				return;
			}

			if(this.applicationHook != null) {
				if(this.applicationHook.beforeService(request, response, ctx) == false) {
					logger.info("App specific hook requested that the service be abandoned after inputting data.");
					return;
				}
			}
			/*
			 * Some possible action between response, context and outSpec.
			 */
			response.beforeService(ctx, service);
			this.initializeResponse(ctx, response, service);
			/*
			 * is this to be run in the background always?
			 * TODO: batch mode
			 */

			/*
			 * TODO : manage cache
			 *
			 * is it cached?
			 */

			if (this.useLocalData) {
				logger.info("Application is set-up to simulate servcie action using local data. Service actions will be ignored");
				this.readLocalData(ctx, service);
			} else {
				logger.info("Control handed over to service");
				service.serve(ctx);
			}
			if (ctx.isInError()) {
				logger.info("service execution returned with errors");
				return;
			}
			if(this.applicationHook != null) {
				if(this.applicationHook.afterService(response, ctx) == false) {
					logger.info("App specific hook aftrer service signalled that we do not output data.");
					return;
				}
			}
			logger.info("Going to write output data");
			this.writeResponse(ctx, service, response);

			/*
			 *
			 * TODO: cache to be invalidated or this is to be cached.
			 */
		} catch (Exception e) {
			logger.error("Exception thrown by service {}, {}" + service.getQualifiedName(), e.getMessage());
			Application.reportApplicationError(request, e);
			ctx.addMessage(Messages.INTERNAL_ERROR, e.getMessage());
		}
	}

	/**
	 * @param ctx
	 * @param response
	 */
	private void initializeResponse(ServiceContext ctx, ServiceResponse response, Service service) {
		OutputData outSpec = service.getOutputSpecification();
		if (outSpec == null || outSpec.isOutputFromWriter() == false) {
			return;
		}
		// outSpec.onServiceStart(ctx); is to be deprecated with this approach
		ResponseWriter writer = null;
		PayloadType pt = response.getPayloadType();
		if (pt.isStream()) {
			if (pt.isJson()) {
				writer = new JsonRespWriter(response.getPayloadStream());
			} else {
				throw new ApplicationError(
						"XML object writer is not yet designed for service to write directly. Use JSON instead.");
			}
		} else if (pt.isJson()) {
			writer = new JsonRespWriter();
		} else {
			throw new ApplicationError(
					"XML object writer is not yet designed for service to write directly. Use JSON instead.");
		}

		ctx.setWriter(writer);
		logger.info(
				"Writer set to service context. Service is expected to write response directly to an object writer.");
	}

	/**
	 * @param ctx
	 * @param service
	 * @param response
	 */
	@SuppressWarnings("resource")
	private void writeResponse(ServiceContext ctx, Service service, ServiceResponse response) {

		PayloadType pt = response.getPayloadType();
		OutputData outSpec = service.getOutputSpecification();
		if (outSpec == null) {
			logger.warn("Service has no output specification and hence no response is emitted.");
			return;
		}

		if (pt == null) {
			logger.warn("Service has output specified, but client is not expecting any output. No response emitted.");
			return;
		}
		ResponseWriter respWriter = null;
		try {
			if (pt.isStream()) {
				if (outSpec.isOutputFromWriter()) {
					logger.info("Service would have output response directly to teh stream.");
					return;
				}

				Writer writer = response.getPayloadStream();
				if (pt.isJson()) {
					respWriter = new JsonRespWriter(writer);
				} else {
					respWriter = new XmlRespWriter(writer);
				}
				outSpec.write(respWriter, ctx);
				if(this.applicationHook != null) {
					this.applicationHook.afterOutput(response, ctx) ;
					logger.info("App specific hook executed aftrer outputting data but before closing the writer.");
				}
				respWriter.writeout(null);
				return;
			}

			if (pt.isJson()) {
				respWriter = new JsonRespWriter();
			} else {
				respWriter = new XmlRespWriter();
			}
			outSpec.write(respWriter, ctx);
			String resposeText = respWriter.getFinalResponseObject().toString();
			response.setPayloadText(resposeText);
		} catch (IOException | XMLStreamException e) {
			throw new ApplicationError(e, "error while writing response");
		}
	}

	/**
	 *
	 * @return true if this application uses a numeric userId. false if it uses
	 *         text/string as userId
	 */
	public boolean userIdIsNumber() {
		return this.numericUserId;
	}

	/**
	 * invalidate any cached response for this service
	 *
	 * @param serviceName
	 */
	public void invalidateCache(String serviceName) {
		if (instance.cacheManager != null) {
			logger.info("Invalidating cache for the service " + serviceName);
			instance.cacheManager.invalidate(serviceName);
		}
	}

	/**
	 * @param serviceName
	 *            non-null fully qualified service name
	 * @param reader
	 *            non-null request reader that can give values for possible key
	 *            names
	 * @param userId
	 *            non-null user id on whose behalf this service is to be
	 *            executed
	 * @return key with which the response for this request may have been
	 *         cached. null if this is not cached.
	 */
	public static String getCachingKey(String serviceName, RequestReader reader, Value userId) {
		Service service = ComponentManager.getService(serviceName);
		if (service.okToCache() == false) {
			return null;
		}
		String[] keys = service.getCacheKeyNames();
		if (keys == null) {
			return Service.createCachingKey(serviceName, null);
		}
		String[] vals = new String[keys.length];
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			Object val;
			if (key.equals(ServiceProtocol.USER_ID)) {
				val = userId;
			} else {
				val = reader.getValue(key);
			}
			if (val != null) {
				vals[i] = val.toString();
			}
		}
		return Service.createCachingKey(serviceName, vals);
	}

	/**
	 * @param ctx
	 * @param service
	 */
	private void readLocalData(ServiceContext ctx, Service service) {
		String res = ComponentType.getComponentRoot() + "data/" + service.getQualifiedName().replace('.', '/') + ".json";
		String text = IoUtil.readResource(res);
		if (text == null) {
			logger.error("Unable to locate data for service at {}. NO data added to context.", res);
			return;
		}
		try {
			JSONObject json = new JSONObject(text);
			JSONObject data = null;
			for (String key : json.keySet()) {
				if (key.equals("*")) {
					data = json.getJSONObject(key);
				} else {
					Expression exp = new Expression(key);
					Value val = exp.evaluate(ctx);
					if (Value.intepretAsBoolean(val)) {
						data = json.getJSONObject(key);
						break;
					}
				}
			}
			if (data == null) {
				logger.error("JSON data does not have an entry for \"*\" ");
			} else {
				JsonUtil.extractAll(data, ctx);
				logger.info("Data extracted from file into serviceContext");
			}
		} catch (Exception e) {
			logger.error("Error while parsing data from file into service context. ERROR: {} ", e.getMessage());
		}
	}
}
