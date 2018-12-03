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
import java.util.HashMap;
import java.util.Map;

import org.simplity.json.JSONObject;
import org.simplity.json.JSONWriter;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.util.XmlUtil;
import org.simplity.service.OutputData;
import org.simplity.service.ServiceContext;
import org.simplity.tp.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @author simplity.org
 *
 */
public class ServiceResponse {
	private static final Logger logger = LoggerFactory.getLogger(ServiceResponse.class);
	private Object payload;
	private PayloadType payloadType = PayloadType.JSON_TEXT;
	private Map<String, Object> sessionFields;
	private ServiceResult result;
	private FormattedMessage[] messages;
	private int executionTime;

	/**
	 * a service response that does not link its response stream
	 *
	 * @param payloadType
	 *            should be other than JSON_STREAM and XML_STREAM
	 */
	public ServiceResponse(PayloadType payloadType) {
		this.payloadType = payloadType;
		if (this.payloadIsStream()) {
			logger.error("payload type of stream should use constructor with Reader");
		}
		this.payloadType = payloadType;
	}

	/**
	 * construct a response for linking output stream
	 *
	 * @param writer
	 *            output stream
	 * @param useJson
	 */
	public ServiceResponse(Writer writer, boolean useJson) {
		this.payloadType = useJson ? PayloadType.JSON_STREAM : PayloadType.XML_STREAM;
		this.payload = writer;
	}

	/**
	 * @return result of this service execution
	 *
	 */
	public ServiceResult getServiceResult() {
		return this.result;
	}

	/**
	 * @param result
	 *            the result of service execution
	 */
	public void setResult(ServiceResult result) {
		this.result = result;
	}

	/**
	 *
	 * @return messages. Contains error/warning/info as well. If the result is
	 *         not ALL_OK, then there will be at least one error message
	 */
	public FormattedMessage[] getMessages() {
		if (this.messages == null) {
			return new FormattedMessage[0];
		}
		return this.messages;
	}

	/**
	 * set messages. If the result is not ALL_OK, then at least one error
	 * message explaining the failure must be included.
	 *
	 * @param messages
	 */
	public void setMessages(FormattedMessage[] messages) {
		this.messages = messages;
	}

	/**
	 *
	 * @return reader, or null if the payload is not a reader
	 */
	public Document getPayloadXml() {
		if (this.payloadType != PayloadType.XML_OBJECT) {
			logger.error("document expected when the pyload type is {}", this.payloadType);
			return null;
		}
		return (Document) this.payload;
	}

	/**
	 *
	 * @return reader, or null if the payload is not a reader
	 */
	public JSONObject getPayloadJson() {
		if (this.payloadType != PayloadType.JSON_OBJECT) {
			logger.error("document expected when the pyload type is {}", this.payloadType);
			return null;
		}
		return (JSONObject) this.payload;
	}

	/**
	 *
	 * @return reader, or null if the payload is not a reader
	 */
	public String getPayloadText() {
		if (this.payloadIsText()) {
			return (String) this.payload;
		}
		logger.error("payload is asked to be text when its type is {}. null returned", this.payloadType);
		return null;
	}

	/**
	 *
	 * @return true if the payload-type is an xml-type
	 */
	public boolean payloadIsStream() {
		return this.payloadType == PayloadType.JSON_STREAM || this.payloadType == PayloadType.XML_STREAM;
	}

	/**
	 *
	 * @return true if the payload-type is a json-type
	 */
	public boolean payloadIsJson() {
		return this.payloadType == PayloadType.JSON_STREAM || this.payloadType == PayloadType.JSON_OBJECT
				|| this.payloadType == PayloadType.JSON_STREAM;
	}

	private boolean payloadIsText() {
		return this.payloadType == PayloadType.JSON_TEXT || this.payloadType == PayloadType.XML_TEXT;
	}

	/**
	 * session fields are to be used by the client agent to set the
	 * Conversation context. These fields are to be added to every subsequent
	 * request. If the value of a field is null, then it means that the field is
	 * to be removed from the session context
	 *
	 * @return fields to be used as session context fields. null or empty, if
	 *         session context is not
	 *         affected by this service
	 */
	public Map<String, Object> getSessionFields() {
		return this.sessionFields;
	}

	/**
	 * @param sessionFields
	 *            the sessionFields to set
	 */
	public void setSessionFields(Map<String, Object> sessionFields) {
		this.sessionFields = sessionFields;
	}

	/**
	 * @param key
	 * @param value
	 */
	public void setSessionField(String key, String value) {
		this.ensureMap();
		this.sessionFields.put(key, value);
	}

	private void ensureMap() {
		if (this.sessionFields == null) {
			this.sessionFields = new HashMap<>();
		}

	}

	/**
	 * @return output stream associated with this response. null if no stream is
	 *         associated.
	 */
	public Writer getPayloadStream() {
		if (this.payloadIsStream()) {
			return (Writer) this.payload;
		}
		logger.error("Response payload is {} but we received a call to get it as a stream", this.payloadType);
		return null;
	}

	/**
	 * @return the payloadType
	 */
	public PayloadType getPayloadType() {
		return this.payloadType;
	}

	/**
	 * set a json object as payload. should be called only once if the payload
	 * is stream
	 *
	 * @param json
	 */
	public void setJsonPayload(JSONObject json) {
		switch (this.payloadType) {
		case JSON_OBJECT:
			logger.info("reponse payload set as object.");
			this.payload = json;
			return;

		case JSON_STREAM:
			JSONWriter writer = new JSONWriter(((Writer) this.payload));
			writer.object();
			writer.value(json);
			writer.endObject();
			logger.info("reponse json is written to output stream.");
			return;

		case JSON_TEXT:
			logger.info("reponse payload set as json text.");
			this.payload = json.toString();
			return;

		default:
			logger.error("Json payload can not be set when payloadType is {}", this.payloadType);
			return;
		}
	}

	/**
	 * set an xml dom as response payload
	 *
	 * @param rootElement
	 */
	public void setXmlPayload(Element rootElement) {
		switch (this.payloadType) {
		case XML_OBJECT:
			logger.info("reponse payload set as object.");
			this.payload = rootElement;
			return;

		case XML_STREAM:
			boolean allOk = XmlUtil.toStream(rootElement, (Writer) this.payload);
			if (allOk) {
				logger.info("Xml output to the writer");
			} else {
				logger.error("XML could not be written to the writer");
			}
			return;

		case XML_TEXT:
			this.payload = XmlUtil.eleToString(rootElement);
			if (this.payload == null) {
				logger.error("XML was not converted to String. payload set to null");
			} else {
				logger.info("Payload set to XML text");
			}
			return;

		default:
			logger.error("XML payload can not be set when payloadType is {}", this.payloadType);
			return;
		}
	}

	/**
	 * set payload text
	 *
	 * @param text
	 *            to be set. must be as per strict syntax of desired response
	 *            type
	 */
	public void setPayloadText(String text) {
		switch (this.payloadType) {
		case XML_TEXT:
		case JSON_TEXT:
			this.payload = text;
			return;

		case XML_STREAM:
		case JSON_STREAM:
			try {
				((Writer) this.payload).write(text);
			} catch (IOException e) {

				throw new ApplicationError(e, "Error while writing response text to payload stream");
			}
			return;
		case JSON_OBJECT:
			this.payload = new JSONObject(text);
			return;

		default:
			throw new ApplicationError("Design not yet ready for a payload type of xml object to accept text");
		}
	}

	/**
	 * @param millis
	 *            no of milli seconds taken by service to process this request
	 */
	public void setExecutionTime(int millis) {
		this.executionTime = millis;
	}

	/**
	 * @return the executionTime
	 */
	public int getExecutionTime() {
		return this.executionTime;
	}

	/**
	 * anything we have to put into context before service starts..
	 *
	 * @param ctx
	 * @param service
	 */
	public void beforeService(ServiceContext ctx, Service service) {
		OutputData outSpec = service.getOutputSpecification();
		if (outSpec == null) {
			return;
		}

		if (outSpec.isOutputFromWriter() == false) {
			return;
		}
		ResponseWriter writer = null;
		if (this.payloadIsJson() == false) {
			throw new ApplicationError("non-json responsewriter is not yet ready to be used directky by service.");
		}
		if (this.payloadIsStream()) {
			writer = new JsonRespWriter(this.getPayloadStream());
		} else {
			writer = new JsonRespWriter();
		}
		logger.info("Started Writer for this service");
		ctx.setWriter(writer);
	}
}
