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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import org.simplity.json.JSONWriter;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.util.JsonUtil;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * prepares a JSON response based on output specifications. Important to note
 * that this writer automatically starts an object so that the caller can start
 * writing right away. It also obviously mean that this root object is
 * automatically closed when the response ends
 *
 * @author simplity.org
 *
 */
public class JsonRespWriter implements ResponseWriter {
	/**
	 * initialized on instantiation. set to null once writer is closed.
	 */
	private JSONWriter writer;
	/**
	 * null till the writer is closed. will be null if the writer was not a
	 * string writer. Keeps the final string, if the writer was not piped to any
	 * other output
	 */
	private String responseText;

	/**
	 * is there an underlying io.writer?
	 */
	private final StringWriter stringWriter;

	/**
	 * crate a string writer.
	 */
	public JsonRespWriter() {
		this.stringWriter = new StringWriter();
		this.writer = new JSONWriter(this.stringWriter);
		this.writer.object();
	}

	/**
	 * crate a string writer.
	 *
	 * @param writer
	 *            that will receive the output
	 */
	public JsonRespWriter(Writer writer) {
		this.stringWriter = null;
		this.writer = new JSONWriter(writer);
		this.writer.object();
	}

	/**
	 * crate a string writer.
	 *
	 * @param stream
	 *            that will receive the output
	 */
	public JsonRespWriter(OutputStream stream) {
		this.stringWriter = null;
		this.writer = new JSONWriter(new OutputStreamWriter(stream));
		this.writer.object();
	}

	/**
	 * get the response text and close the writer. That is the reason this
	 * method is called getFinalResponse rather than getResponse. Side effect of
	 * closing the writer is hinted with this name
	 *
	 * @return response text. Null if nothing was written so far, or the writer
	 *         was piped to another output mechanism
	 */
	@Override
	public String getFinalResponseObject() {
		if (this.writer == null) {
			/*
			 * it was already closed..
			 */
			return this.responseText;
		}
		/*
		 * close writer
		 */
		this.writer.endObject();

		/*
		 * get final text into responseText
		 */
		if (this.stringWriter != null) {
			this.responseText = this.stringWriter.toString();
		} else {
			// we were just a pipe, we do not have the accumulated string. That
			// is by design, and hence caller should be aware. Prefer empty
			// string to null
			this.responseText = "";
		}
		this.writer = null;
		return this.responseText;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#writeout(java.io.Writer)
	 */
	@Override
	public void writeout(Writer outWriter) throws IOException {
		if (this.writer == null) {
			/*
			 * stream was already closed
			 */
			return;
		}
		if (outWriter == null) {
			return;
		}
		Object obj = this.getFinalResponseObject().toString();
		if (obj != null) {
			outWriter.write(obj.toString());
		}
	}

	/**
	 * every call to write requires us to check if teh writer is still open
	 */
	private void checkNull() {
		if (this.writer == null) {
			throw new ApplicationError("Response writer is invoked after the writer is closed.");
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#field(java.lang.String,
	 * org.simplity.kernel.value.Value)
	 */
	@Override
	public void setField(String fieldName, Object value) {
		this.checkNull();
		this.writer.key(fieldName).value(value);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#setField(java.lang.String,
	 * com.google.protobuf.Value)
	 */
	@Override
	public void setField(String fieldName, Value value) {
		this.checkNull();
		this.writer.key(fieldName).value(value);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#field(java.lang.Object)
	 */
	@Override
	public void addToArray(Object value) {
		this.checkNull();
		this.writer.value(value);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#object(java.lang.String,
	 * java.lang.Object)
	 */
	@Override
	public void setObject(String fieldName, Object value) {
		this.checkNull();

		this.writer.key(fieldName);
		JsonUtil.addObject(this.writer, value);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#array(java.lang.String,
	 * java.lang.Object[])
	 */
	@Override
	public void setArray(String arrayName, Object[] arr) {
		this.checkNull();
		this.writer.key(arrayName).array();
		if (arr != null && arr.length != 0) {
			for (Object value : arr) {
				JsonUtil.addObject(this.writer, value);
			}
		}
		this.writer.endArray();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#array(java.lang.String,
	 * org.simplity.kernel.data.DataSheet)
	 */
	@Override
	public void setArray(String arrayName, DataSheet sheet) {
		this.checkNull();
		this.writer.array();
		if (sheet != null && sheet.length() > 0 && sheet.width() > 0) {
			for (Value[] row : sheet.getAllRows()) {
				Value value = row[0];
				if (value != null) {
					this.writer.value(value);
				}
			}
		}
		this.writer.endArray();

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginObject(java.lang.String)
	 */
	@Override
	public JsonRespWriter beginObject(String objectName) {
		this.checkNull();
		this.writer.key(objectName).object();
		return this;

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginObject(java.lang.String)
	 */
	@Override
	public JsonRespWriter beginObjectAsArrayElement() {
		this.checkNull();
		this.writer.object();
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#endObject()
	 */
	@Override
	public JsonRespWriter endObject() {
		this.checkNull();
		this.writer.endObject();
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginArray(java.lang.String)
	 */
	@Override
	public JsonRespWriter beginArray(String arrayName) {
		this.checkNull();
		this.writer.key(arrayName).array();
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginArray()
	 */
	@Override
	public JsonRespWriter beginArrayAsArrayElement() {
		this.checkNull();
		this.writer.array();
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#endArray()
	 */
	@Override
	public JsonRespWriter endArray() {
		this.checkNull();
		this.writer.endArray();
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#writeAsPerSpec(org.simplity.service.
	 * ServiceContext)
	 */
	@Override
	public void pullDataFromContext(ServiceContext ctx) {
		this.checkNull();
		/*
		 * we are to write based on our spec
		 */
		for (Map.Entry<String, Value> entry : ctx.getAllFields()) {
			this.setField(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, DataSheet> entry : ctx.getAllSheets()) {
			DataSheet sheet = entry.getValue();
			if (sheet == null || sheet.length() == 0) {
				continue;
			}

			this.beginArray(entry.getKey());

			int nbrRows = sheet.length();
			String[] names = sheet.getColumnNames();
			for (int i = 0; i < nbrRows; i++) {
				Value[] row = sheet.getRow(i);
				this.beginObjectAsArrayElement();
				for (int j = 0; j < names.length; j++) {
					this.setField(names[j], row[j]);
				}
				this.endObject();
			}
			this.endArray();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#hasOutputSpec()
	 */
	@Override
	public boolean hasOutputSpec() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#useAsResponse(java.lang.Object)
	 */
	@Override
	public void setAsResponse(Object responseObject) {
		throw new ApplicationError(
				"This writer streams data as and when it is writtern, and hance can not just set an object as response.");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#moveToField(java.lang.String)
	 */
	@Override
	public Object moveToObject(String qualifiedFieldName) {
		throw new ApplicationError(
				"This writer streams data as and when it is writtern, and hance can not re-position.");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#moveToArray(java.lang.String)
	 */
	@Override
	public Object moveToArray(String qualifiedFieldName) {
		throw new ApplicationError("This writer can not move round the object tree.");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#setAsCurrentObject(java.lang.Object)
	 */
	@Override
	public void setAsCurrentObject(Object object) {
		throw new ApplicationError("This writer can not move round the object tree.");
	}
}
