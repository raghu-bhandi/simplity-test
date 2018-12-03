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
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * prepares an XML response based on output specifications. Important to note
 * that this writer automatically starts an object so that the caller can start
 * writing right away. It also obviously means that this root object is
 * automatically closed when the response ends. And of course, you can not write
 * a fragment that is not an object
 *
 * @author simplity.org
 *
 */
public class XmlRespWriter implements ResponseWriter {
	private static final Logger logger = LoggerFactory.getLogger(XmlRespWriter.class);
	private static final String ARRAY_TAG_NAME = "elements";
	/**
	 * Streams the XML as it is created. initialized on instantiation. set to
	 * null once writer is closed.
	 */
	private XMLStreamWriter xmlWriter;

	/**
	 * if no writer is piped, we need a stringWriter of our own.
	 */
	private final StringWriter stringWriter;

	/**
	 * Actual text of the XML. relevant only if this is no piped to an
	 * underlying writer. Value is set only after a call to close() till the
	 * writer is closed. will be null if the
	 */
	private String responseText;

	private String arrayTagName = ARRAY_TAG_NAME;
	/**
	 * crate a string writer. this writer is not piped to any existing writer
	 *
	 * @throws XMLStreamException
	 */
	public XmlRespWriter() throws XMLStreamException {
		this.stringWriter = new StringWriter();
		this.xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(this.stringWriter);
		this.xmlWriter.writeStartDocument();
	}

	/**
	 * crate a xml writer that uses the underlying writer
	 *
	 * @param riter
	 *            underlying writer that will be wrapped as XMLStreamWriter
	 *            that will receive the output
	 * @throws XMLStreamException
	 */
	public XmlRespWriter(Writer riter) throws XMLStreamException {
		this.stringWriter = null;
		this.xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(riter);
		this.xmlWriter.writeStartDocument();
	}

	/**
	 * crate a xml writer that uses the underlying writer
	 *
	 * @param stream
	 *            underlying writer that will be wrapped as XMLStreamWriter
	 *            that will receive the output
	 * @throws XMLStreamException
	 */
	public XmlRespWriter(OutputStream stream) throws XMLStreamException {
		this.stringWriter = null;
		this.xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(stream);
		this.xmlWriter.writeStartDocument();
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
	public Object getFinalResponseObject() {
		if (this.xmlWriter == null) {
			/*
			 * it was already closed..
			 */
			return this.responseText;
		}
		/*
		 * close writer
		 */
		try {
			this.xmlWriter.writeEndDocument();
		} catch (XMLStreamException e) {
			logger.error("Error while writing the end document. {}. empty string returned as final text.",
					e.getMessage());
			return "";
		}

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
		this.xmlWriter = null;
		return this.responseText;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.gateway.RespWriter#writeout(java.io.Writer)
	 */
	@Override
	public void writeout(Writer outWriter) throws IOException {
		if (this.xmlWriter == null) {
			/*
			 * stream was already closed
			 */
			return;
		}
		if(outWriter == null) {
			try {
				this.xmlWriter.flush();
			} catch (XMLStreamException e) {
				throw new ApplicationError(e, "XML error");
			}
			return;
		}

		Object obj = this.getFinalResponseObject();
		if(obj != null) {
			outWriter.write(obj.toString());
		}
	}

	/**
	 * every call to write requires us to check if the writer is still open
	 */
	private void checkNull() {
		if (this.xmlWriter == null) {
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
		try {
			this.xmlWriter.writeStartElement(fieldName);
			this.xmlWriter.writeCharacters(value.toString());
			this.xmlWriter.writeEndElement();
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "Error while writing out put for field name " + fieldName);
		}
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
		try {
			this.xmlWriter.writeStartElement(fieldName);
			this.xmlWriter.writeCharacters(value.toString());
			this.xmlWriter.writeEndElement();
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "Error while writing out put for field name " + fieldName);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#field(java.lang.Object)
	 */
	@Override
	public void addToArray(Object value) {
		this.checkNull();
		if (value instanceof Value == false) {
			logger.warn("Xml writer can not write arbitrary objects. array element is assumed to be primitive");
			return;
		}
		this.setField(this.arrayTagName, (Value) value);
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
		throw new ApplicationError(
				"XmlResponseWriter is not designed to write arbitrary objects. Caller has to use lower level methods.");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#array(java.lang.String,
	 * java.lang.Object[])
	 */
	@Override
	public void setArray(String arrayName, Object[] arr) {
		if (arr == null) {
			return;
		}
		this.checkNull();
		this.beginArray(arrayName);
		int nbr = arr.length;
		if (nbr > 0) {
			logger.warn("XmlResponseWriter can not write array of objects directly. primitive value is assumed.");
			for (Object obj : arr) {
				this.setField(arrayName, obj);
			}
		}
		this.endArray();
		return;
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
		this.beginArray(arrayName);
		String[] fieldNames = sheet.getColumnNames();
		int nbrRows = sheet.length();
		for (int i = 0; i < nbrRows; i++) {
			this.setRow(arrayName, fieldNames, sheet.getRow(i));
		}
		this.endArray();
	}

	/**
	 * @param arrayName
	 * @param fieldNames
	 * @param row
	 */
	private void setRow(String arrayName, String[] fieldNames, Value[] row) {
		this.beginObject(arrayName);
		for (int i = 0; i < fieldNames.length; i++) {
			this.setField(fieldNames[i], row[i]);
		}
		this.endObject();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginObject(java.lang.String)
	 */
	@Override
	public XmlRespWriter beginObject(String objectName) {
		this.checkNull();
		try {
			this.xmlWriter.writeStartElement(objectName);
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
		return this;

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginObject(java.lang.String)
	 */
	@Override
	public XmlRespWriter beginObjectAsArrayElement() {
		this.checkNull();
		try {
			this.xmlWriter.writeStartElement(this.arrayTagName);
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#endObject()
	 */
	@Override
	public XmlRespWriter endObject() {
		this.checkNull();
		try {
			this.xmlWriter.writeEndElement();
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginArray(java.lang.String)
	 */
	@Override
	public XmlRespWriter beginArray(String arrayName) {
		this.checkNull();
		this.arrayTagName = arrayName;
		try {
			this.xmlWriter.writeStartElement(arrayName);
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#beginArray()
	 */
	@Override
	public XmlRespWriter beginArrayAsArrayElement() {
		this.checkNull();
		try {
			this.xmlWriter.writeStartElement(this.arrayTagName);
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.service.OutputWriter#endArray()
	 */
	@Override
	public XmlRespWriter endArray() {
		this.checkNull();
		this.arrayTagName = ARRAY_TAG_NAME;
		try {
			this.xmlWriter.writeEndElement();
		} catch (XMLStreamException e) {
			throw new ApplicationError(e, "error while writing xml stream");
		}
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
