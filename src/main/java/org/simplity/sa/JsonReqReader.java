/*
 * Copyright (c) 2017 simplity.org
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

import java.util.Stack;

import org.simplity.json.JSONArray;
import org.simplity.json.JSONObject;
import org.simplity.kernel.util.JsonUtil;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * request reader for json input
 *
 * @author simplity.org
 *
 */
public class JsonReqReader implements RequestReader {
	private static final Logger logger = LoggerFactory.getLogger(JsonReqReader.class);
	/**
	 * payload parsed into a JSON Object. Null if input is not a json
	 */
	private final JSONObject inputJson;
	/**
	 * raw payload. null if it is parsed into a valid json
	 */
	private final String inputText;

	/**
	 * stack of open objects.
	 */
	private Stack<Object> openObjects = new Stack<Object>();

	/**
	 * current object that is open. null if the current object is an array
	 */
	private JSONObject currentObject;

	/**
	 * current array that is open. null if the current object is a OBJECT.
	 */
	private JSONArray currentArray;

	/**
	 * instantiate a translator for the input payload
	 *
	 * @param payload
	 */
	public JsonReqReader(String payload) {
		if (payload == null || payload.isEmpty()) {
			logger.info("Input is empty for translator.");
			this.inputJson = null;
			this.inputText = null;
			return;
		}
		JSONObject json = null;
		try {
			json = new JSONObject(payload);
		} catch (Exception e) {
			logger.info("Input is not a valid json. We treat that as a single value");
		}
		if (json == null) {
			this.inputJson = null;
			this.inputText = payload;
		} else {
			this.currentObject = this.inputJson = json;
			this.inputText = null;
		}
	}

	/**
	 * instantiate input translator for a json
	 *
	 * @param json
	 */
	public JsonReqReader(JSONObject json) {
		this.currentObject = this.inputJson = json;
		this.inputText = null;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.simplity.service.DataTranslator#saveRawInput(org.simplity.service.
	 * ServiceContext, java.lang.String)
	 */
	@Override
	public Object getRawInput() {
		if (this.inputText != null) {
			return this.inputText;
		}
		return this.inputJson;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.DataTranslator#getValueType(java.lang.String)
	 */
	@Override
	public InputValueType getValueType(String fieldName) {
		if (this.currentObject == null) {
			return InputValueType.NULL;
		}
		return getType(this.currentObject.opt(fieldName));
	}

	private static InputValueType getType(Object val) {
		if (val == null) {
			return InputValueType.NULL;
		}
		if (val instanceof JSONArray) {
			return InputValueType.ARRAY;
		}
		if (val instanceof JSONObject) {
			return InputValueType.OBJECT;
		}
		return InputValueType.VALUE;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#getValueType(int)
	 */
	@Override
	public InputValueType getValueType(int idx) {
		if (this.currentArray == null) {
			return InputValueType.NULL;
		}
		return getType(this.currentArray.opt(idx));
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.DataTranslator#getValue(java.lang.String)
	 */
	@Override
	public Object getValue(String fieldName) {
		if (this.currentObject == null) {
			return null;
		}
		return this.currentObject.opt(fieldName);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#openObject(java.lang.String)
	 */
	@Override
	public boolean openObject(String attributeName) {
		if (this.currentObject == null) {
			logger.error("There is no current object and hence openObject request for {} is denied.", attributeName);
			return false;
		}

		Object obj = this.currentObject.opt(attributeName);
		if (obj == null) {
			logger.error("Current object has no attribute named {}. openObject request is denied.", attributeName);
			return false;
		}

		if (obj instanceof JSONObject == false) {
			logger.error("Attribute named {} is of type {}. openObject request is denied.", attributeName,
					obj.getClass().getName());
			return false;
		}

		this.openObjects.push(this.currentObject);
		this.currentObject = (JSONObject) obj;
		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#openObject(int)
	 */
	@Override
	public boolean openObject(int idx) {
		if (this.currentArray == null) {
			logger.error("current object is no an array and hence openObject request for {} is denied.", idx);
			return false;
		}

		Object obj = this.currentArray.opt(idx);
		if (obj == null) {
			logger.error("Current array has no attribute at {}. openObject request is denied.", idx);
			return false;
		}

		if (obj instanceof JSONObject == false) {
			logger.error("Element at index {} is of type {}. openObject request is denied.", idx,
					obj.getClass().getName());
			return false;
		}

		this.openObjects.push(this.currentArray);
		this.currentObject = (JSONObject) obj;
		this.currentArray = null;
		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#closeObject()
	 */
	@Override
	public boolean closeObject() {
		if (this.currentObject == null) {
			logger.error("closeObject() out of sequence with its openObject()");
			return false;
		}
		return this.pop();
	}

	private boolean pop() {
		if (this.openObjects.isEmpty()) {
			return false;
		}
		Object obj = this.openObjects.pop();
		if (obj instanceof JSONObject) {
			this.currentObject = (JSONObject) obj;
			this.currentArray = null;
		} else {
			this.currentArray = (JSONArray) obj;
			this.currentObject = null;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#openArray(java.lang.String)
	 */
	@Override
	public boolean openArray(String attributeName) {
		if (this.currentObject != null) {
			Object obj = this.currentObject.opt(attributeName);
			if (obj != null && obj instanceof JSONArray) {
				this.openObjects.push(this.currentObject);
				this.currentObject = null;
				this.currentArray = (JSONArray) obj;
				return true;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#openArray(int)
	 */
	@Override
	public boolean openArray(int zeroBasedIdx) {
		if (this.currentArray != null) {
			Object obj = this.currentArray.opt(zeroBasedIdx);
			if (obj != null && obj instanceof JSONArray) {
				this.openObjects.push(this.currentArray);
				this.currentArray = (JSONArray) obj;
				return true;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#endArray()
	 */
	@Override
	public boolean closeArray() {
		if (this.currentArray == null) {
			return false;
		}
		return this.pop();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#getValue(int)
	 */
	@Override
	public Object getValue(int zeroBasedIdx) {
		if (this.currentArray == null) {
			return null;
		}
		return this.currentArray.opt(zeroBasedIdx);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#getNbrElements()
	 */
	@Override
	public int getNbrElements() {
		if (this.currentArray == null) {
			return 0;
		}
		return this.currentArray.length();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.ReqReader#getAttributeNames()
	 */
	@Override
	public String[] getAttributeNames() {
		if (this.currentObject == null) {
			return new String[0];
		}
		return JSONObject.getNames(this.currentObject);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.ReqReader#readAll(org.simplity.service.
	 * ServiceContext)
	 */
	@Override
	public void pushDataToContext(ServiceContext ctx) {
		if (this.inputJson == null) {
			logger.info("No input json assigned to the reader before extracting data.");
			return;
		}
		JsonUtil.extractAll(this.inputJson, ctx);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.ReqReader#hasInputSpecs()
	 */
	@Override
	public boolean hasInputSpecs() {
		return true;
	}
}
