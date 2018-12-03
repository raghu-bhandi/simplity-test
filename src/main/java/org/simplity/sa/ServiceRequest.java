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
import java.io.Reader;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.simplity.json.JSONObject;
import org.simplity.kernel.MessageBox;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.util.IoUtil;
import org.simplity.kernel.value.Value;
import org.simplity.service.InputData;
import org.simplity.service.ServiceContext;
import org.simplity.tp.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author simplity.org
 *
 */
public class ServiceRequest {
	private static final Logger logger = LoggerFactory.getLogger(ServiceRequest.class);
	private static DocumentBuilderFactory domFactory = null;

	/**
	 * service requested by client
	 */
	private String serviceName;

	/**
	 * user for whom this request is made
	 */
	private AppUser appUser;

	/**
	 * paylaod type
	 */
	private PayloadType payloadType;
	/**
	 * actual payload. Special case is when payloadType us null and payload is
	 * ServiceContext, used in internal service calls
	 */
	private Object payload;
	/**
	 * fields that are meant to be from client, but are not in payload. like the
	 * fields in REST url, query strings, cookies and session fields
	 */
	private Map<String, Object> fields;

	/**
	 * special arrangement for service to communicate its progress to the
	 * requester. Used in batch case
	 */
	private MessageBox messageBox = null;

	/**
	 * context maintained on client layer (typically web/servlet). This is a big
	 * NO from our side because it holds us back from hosting the two layers on
	 * different JVMs.
	 * Provisioned for us to co-exist with Apps that need this feature.
	 */
	private Object clientContext;

	/**
	 *
	 * @param serviceName
	 *            non-null string
	 * @param payloadType
	 *            null if no payload.
	 * @param payload
	 *            appropriate object based on payload type. Reader if
	 *            JSON_STREAM, XML_STREAM, JsonObject if
	 *            JSON_OBJECT, Document if XML_OBJECT, String for JSON_TEXT or
	 *            XML_TEXT. Object ignored after logging an error in case of
	 *            mismatch, but no exception is thrown
	 */
	public ServiceRequest(String serviceName, PayloadType payloadType, Object payload) {
		this.serviceName = serviceName;
		this.payloadType = payloadType;
		if (payloadType == null) {
			return;
		}
		this.payload = payload;
		/*
		 * flash error in case payload is not as per specified type
		 */
		switch (payloadType) {
		case JSON_TEXT:
		case XML_TEXT:
			if (payload instanceof String) {
				return;
			}
			break;
		case JSON_OBJECT:
			if (payload instanceof JSONObject) {
				return;
			}
			break;
		case JSON_STREAM:
		case XML_STREAM:
			if (payload instanceof Reader) {
				return;
			}
			break;
		case XML_OBJECT:
			if (payload instanceof Document) {
				return;
			}
			break;
		default:
			this.payloadType = null;
			this.payload = null;
			break;
		}
		logger.error("payloadType= {} but the actual payload is {}. payload is set to null.", payloadType,
				payload.getClass().getName());
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return this.serviceName;
	}

	/**
	 * @return the payloadType. null if no payload was set.
	 */
	public PayloadType getPayloadType() {
		return this.payloadType;
	}

	/**
	 *
	 * @return pay load as it is available. Could be null
	 */
	public Object getPayload() {
		return this.payload;
	}

	/**
	 *
	 * @return payload as json. null if payload type is not set to json
	 */
	public JSONObject getPayloadJson() {
		if (this.payloadType == PayloadType.JSON_OBJECT) {
			return (JSONObject) this.payload;
		}
		String json = null;
		if (this.payloadType == PayloadType.JSON_STREAM) {
			json = IoUtil.readerToText((Reader) this.payload);
			if (json == null) {
				logger.error("payload read failed to get any text. assuming no input from payload");
				return new JSONObject();
			}
			if (json.isEmpty()) {
				logger.info("Payload is empty");
				return new JSONObject();
			}
		} else if (this.payloadType == PayloadType.JSON_TEXT) {
			json = this.payload.toString();
		} else {
			logger.error("json requested when the payload is not a json. returning null");
			return null;
		}

		try {
			return new JSONObject(json);
		} catch (Exception e) {
			logger.error("JSON error : {}\n Payload is:\n{}.", e.getMessage(), json);
			return new JSONObject();
		}
	}

	/**
	 *
	 * @return xml document, either from text or input stream. null if payload
	 *         type is set to non-xml
	 */
	public Document getPayloadXml() {
		if (this.payloadType == PayloadType.XML_OBJECT) {
			return (Document) this.payload;
		}

		if (this.payloadType != PayloadType.XML_STREAM && this.payloadType != PayloadType.XML_TEXT) {
			logger.error("xml requested when the payload is not an xml. returning null");
			return null;
		}

		if (domFactory == null) {
			this.setDomFactory();
		}
		DocumentBuilder domBuilder = null;
		try {
			domBuilder = domFactory.newDocumentBuilder();
			if (this.payloadType == PayloadType.XML_TEXT) {
				return domBuilder.parse(this.payload.toString());
			}
			return domBuilder.parse(new InputSource((Reader) this.payload));
		} catch (SAXException | IOException | ParserConfigurationException e) {
			logger.error("Unable to parse xml document", e);
			return null;
		}
	}

	/**
	 *
	 * @return payload as plain text
	 */
	public String getPayloadText() {
		switch (this.payloadType) {
		case JSON_STREAM:
		case XML_STREAM:
			return this.readAll();

		case JSON_TEXT:
		case XML_TEXT:
			return (String) this.payload;

		case JSON_OBJECT:
			return ((JSONObject) this.payload).toString();
		case XML_OBJECT:
			try {
				Transformer transformer = TransformerFactory.newInstance().newTransformer();
				StringWriter writer = new StringWriter();
				transformer.transform(new DOMSource((Document) this.payload), new StreamResult(writer));
				return writer.toString();
			} catch (Exception e) {
				logger.error("Error while converting paylod xml object to text. {} ", e.getMessage());
				return null;
			}
		default:
			break;
		}
		logger.error("getPayloadText() is not designed to handle payloadtype {}", this.payload);
		return null;
	}

	/**
	 *
	 * @param fieldName
	 * @return value of the named field, or null if no such field
	 */
	public Object getFieldValue(String fieldName) {
		if (this.fields == null) {
			return null;
		}
		return this.fields.get(fieldName);
	}

	/**
	 *
	 * @return set containing all the fields. empty set if there are no fields
	 */
	public Set<String> getFieldNames() {
		if (this.fields == null) {
			return new HashSet<>();
		}
		return this.fields.keySet();
	}

	/**
	 * @param fields
	 *            the fields to set
	 */
	public void setFields(Map<String, Object> fields) {
		this.fields = fields;
	}

	/**
	 * set/remove value for a field
	 *
	 * @param fieldName
	 *            name of the field
	 * @param fieldValue
	 *            value of the field to set. if null, current value, removed.
	 * @return current value of this field
	 */
	public Object setFieldValue(String fieldName, Object fieldValue) {
		if (fieldValue == null) {
			return this.fields.remove(fieldName);
		}
		return this.fields.put(fieldName, fieldValue);
	}

	/**
	 * @param user
	 *            the user to set
	 */
	public void setUser(AppUser user) {
		this.appUser = user;
	}

	/**
	 * @return the userId
	 */
	public AppUser getUser() {
		return this.appUser;
	}

	/**
	 * @param messageBox
	 *            the messageBox to set
	 */
	public void setMessageBox(MessageBox messageBox) {
		this.messageBox = messageBox;
	}

	/**
	 * @return the messageBox
	 */
	public MessageBox getMessageBox() {
		return this.messageBox;
	}

	/**
	 * @param clientContext
	 *            context maintained on client layer (typically web/servlet).
	 *            This is a big NO from our side because it holds us back from
	 *            hosting the two layers on different JVMs.
	 *            Provisioned for us to co-exist with Apps that need this
	 *            feature. This will be available in service context with
	 *            getClientContext() method
	 */
	public void setClientContext(Object clientContext) {
		this.clientContext = clientContext;
	}

	/**
	 * @return the clientContext
	 */
	public Object getClientContext() {
		return this.clientContext;
	}

	/**
	 * copy whatever we are carrying for the service layer.
	 *
	 * @param ctx
	 *            service context
	 * @param service
	 *            service being served
	 */
	public void copyToServiceContext(ServiceContext ctx, Service service) {
		/*
		 * fields are generally routinely copied, without any request from
		 * service layer to do so.
		 * TODO: what if any data expectations of inSpec in fields??
		 * As of now, we are copying the fields to ctx first, so that inSpec can
		 * choose to check context
		 */
		for (Map.Entry<String, Object> entry : this.fields.entrySet()) {
			String key = entry.getKey();
			Object val = entry.getValue();
			if (val instanceof String) {
				ctx.setTextValue(key, (String) val);
			} else if (val instanceof Value) {
				ctx.setValue(key, (Value) val);
			} else if (val instanceof DataSheet) {
				ctx.putDataSheet(key, (DataSheet) val);
			} else {
				ctx.setObject(key, val);
			}
		}

		if (this.clientContext != null) {
			ctx.setClientContext(this.clientContext);
		}

		if (this.messageBox != null) {
			ctx.setMessageBox(this.messageBox);
		}

		InputData inSpec = service.getInputSpecification();
		if (inSpec == null) {
			if (this.payloadType != null && this.payload != null) {
				logger.warn("Service has no input specification and hence input layload is ignored.");
			}
			return;
		}

		RequestReader reader = null;
		if (this.payloadType == null || this.payload == null) {
			logger.warn(
					"Service is expecting data, but payload is empty. Default values specified in specifications, if any, will be used as input.");
			reader = new JsonReqReader(new JSONObject());
		} else {
			if (this.payloadType.isJson()) {
				logger.info("Input being read as JSON");
				reader = new JsonReqReader(this.getPayloadJson());
			} else {
				logger.info("Input being read as XML");
				reader = new XmlReqReader(this.getPayloadXml());
			}
		}
		inSpec.read(reader, ctx);
	}

	private void setDomFactory() {
		domFactory = DocumentBuilderFactory.newInstance();
		domFactory.setValidating(false);
		domFactory.setIgnoringComments(true);
		domFactory.setIgnoringElementContentWhitespace(true);
		domFactory.setNamespaceAware(false);
	}

	private String readAll() {
		StringBuilder sbf = new StringBuilder();
		try (Reader reader = (Reader) this.payload) {
			int ch;
			while ((ch = reader.read()) > -1) {
				sbf.append((char) ch);
			}
			return sbf.toString();
		} catch (Exception e) {
			logger.error("Error while reading request payload. {} ");
			return "";
		}
	}
}
