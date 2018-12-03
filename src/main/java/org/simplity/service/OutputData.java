/*
 * Copyright (c) 2016 simplity.org
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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.simplity.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.dm.Field;
import org.simplity.kernel.util.TextUtil;
import org.simplity.kernel.value.Value;
import org.simplity.sa.ResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component that specifies what inputs are expected
 *
 * @author simplity.org
 *
 */

public class OutputData {
	private static final Logger logger = LoggerFactory.getLogger(OutputData.class);

	static final String EMPTY_RESPONSE = "{\"" + ServiceProtocol.REQUEST_STATUS + "\":\"" + ServiceProtocol.STATUS_OK
			+ "\"}";
	/**
	 * no need to extract data for response. This field has the response text
	 * ready
	 */
	String responseTextFieldName;
	/**
	 * extract everything from context and output. Used for testing etc..
	 */
	boolean justOutputEveryThing;
	/**
	 * Service has already taken care of writing to a writer. If this is piped,
	 * then there is no need to output. Else content from this writer is to be
	 * re-written
	 */
	boolean outputFromWriter;

	/**
	 * comma separated list of fields to be output. Use this feature for fields
	 * that have the same internal and external names
	 */
	String[] fieldNames;

	/**
	 * if the output names are different from internal name, we can use
	 * OutputField array that allows this feature.
	 */
	OutputField[] outputFields;
	/**
	 * comma separated list of arrays. Values for arrays are in data sheet with
	 * a single column
	 */
	String[] arrayNames;
	/**
	 * sheets/fields to be output based on record definitions
	 */
	OutputRecord[] outputRecords;

	/**
	 * comma separated data sheets to be output
	 */
	String[] dataSheets;

	/**
	 * comma separated list of objects to be output
	 */
	String[] objectNames;
	/**
	 * if this service wants to set/reset some session fields. Note that this
	 * directive is independent of fieldNames or outputRecords. That is if a is
	 * set as sessionFields, it is not sent to client, unless "a" is also
	 * specified as fieldNames
	 */
	String[] sessionFields;

	/**
	 * comma separated list of field names that carry key to attachments. these
	 * are processed as per attachmentManagement, and revised key is replaced as
	 * the field-value
	 */
	String[] attachmentFields;
	/**
	 * comma separated list of column names in the form
	 * sheetName.columnName,sheetName1.columnName2....
	 */
	String[] attachmentColumns;

	/**
	 * @param outputRecords
	 *            the outputRecords to set
	 */
	public void setOutputRecords(OutputRecord[] outputRecords) {
		this.outputRecords = outputRecords;
	}

	/**
	 * get ready for a long-haul service :-)
	 */
	public void getReady() {
		if (this.outputRecords == null) {
			return;
		}

		Set<String> parents = new HashSet<>();
		if (this.outputRecords != null) {
			for (OutputRecord rec : this.outputRecords) {
				rec.getReady();
				String parentSheetName = rec.parentSheetName;
				if (parentSheetName != null) {
					parents.add(parentSheetName);
				}
			}
		}

		if (parents.size() == 0) {
			return;
		}
		/*
		 * OK. we have to deal with hierarchical data output
		 */
		List<OutputRecord> children = new ArrayList<>();
		OutputRecord[] empty = new OutputRecord[0];
		for (String parentSheetName : parents) {
			children.clear();
			OutputRecord parent = null;
			for (OutputRecord rec : this.outputRecords) {
				if (rec.name.equals(parentSheetName)) {
					parent = rec;
				} else if (parentSheetName.equals(rec.parentSheetName)) {
					children.add(rec);
				}
			}
			if (parent == null) {
				throw new ApplicationError("Parent sheet name" + parentSheetName + " is missing from output records");
			}
			parent.setChildren(children.toArray(empty));
		}
	}

	/**
	 * validate this specification
	 *
	 * @param vtx
	 */
	public void validate(ValidationContext vtx) {
		/*
		 * duplicate field names
		 */
		if (this.fieldNames != null && this.fieldNames.length > 0) {
			Set<String> keys = new HashSet<String>();
			for (String key : this.fieldNames) {
				if (keys.add(key) == false) {
					vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
							key + " is a duplicate field name for output.", "fieldNames"));
				}
			}
		}
		/*
		 * duplicate data sheets?
		 */
		if (this.dataSheets != null && this.dataSheets.length > 0) {
			Set<String> keys = new HashSet<String>();
			for (String key : this.dataSheets) {
				if (keys.add(key) == false) {
					vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
							key + " is a duplicate sheet name for output.", "dataSheets"));
				}
			}
		}

		if (this.outputRecords == null) {
			return;
		}

		/*
		 * validate output records, and also keep sheet-record mapping for other
		 * validations.
		 */
		Map<String, OutputRecord> allSheets = new HashMap<String, OutputRecord>();
		int nbrParents = 0;
		for (OutputRecord rec : this.outputRecords) {
			rec.validate(vtx);
			allSheets.put(rec.name, rec);
			if (rec.parentSheetName != null) {
				nbrParents++;
			}
		}

		if (nbrParents == 0) {
			return;
		}
		/*
		 * any infinite loops with cyclical relationships?
		 */
		for (OutputRecord rec : this.outputRecords) {
			if (rec.parentSheetName != null) {
				this.validateParent(rec, allSheets, vtx);
			}
		}
	}

	private void validateParent(OutputRecord outRec, Map<String, OutputRecord> allSheets, ValidationContext vtx) {
		/*
		 * check for existence of parent, as well
		 */
		Set<String> parents = new HashSet<String>();
		String sheet = outRec.name;
		String parent = outRec.parentSheetName;
		while (true) {
			OutputRecord rec = allSheets.get(parent);
			/*
			 * do we have the parent?
			 */
			if (rec == null) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR, "output sheet " + sheet
						+ " uses parentSheetName=" + parent
						+ " but that sheet name is not used in any outputRecord. Note that all sheets that aprticipate iin parent-child relationship must be defined using outputRecord elements.",
						"parentSheetName"));
				return;
			}
			/*
			 * are we cycling in a circle?
			 */
			if (parents.add(parent) == false) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"output record with sheetName=" + sheet + " has its parentSheetName set to " + parent
								+ ". This is creating a cyclical child-parent relationship.",
						"sheetName"));
				return;
			}
			/*
			 * is the chain over?
			 */
			if (rec.parentSheetName == null) {
				/*
				 * we are fine with this outoutRecord
				 */
				return;
			}
			sheet = rec.name;
			parent = rec.parentSheetName;
		}
	}

	/**
	 * check whether fieldNames of this specification clashes with fields from a
	 * record
	 *
	 * @param fields
	 * @return
	 */
	boolean okToOutputFieldsFromRecord(Field[] fields) {
		if (this.fieldNames == null || this.fieldNames.length == 0 || fields == null || fields.length == 0) {
			return true;
		}

		Set<String> allNames = new HashSet<String>(this.fieldNames.length);
		for (String aName : this.fieldNames) {
			allNames.add(aName);
		}
		for (Field field : fields) {
			if (allNames.contains(field.getName())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * enable output from a writer
	 */
	public void enableOutputFromWriter() {
		this.outputFromWriter = true;

	}

	/**
	 * is this service designed to write to a writer during its execution,
	 * rather than use output specification of dta elements?
	 *
	 * @return true if response is already written to the writer in the context.
	 *         false otherwise
	 */
	public boolean isOutputFromWriter() {
		return this.outputFromWriter;
	}

	/**
	 * set response and session parameters
	 *
	 * @param writer
	 *
	 * @param ctx
	 */
	public void write(ResponseWriter writer, ServiceContext ctx) {
		if (this.outputFromWriter) {
			ResponseWriter ctxWriter = ctx.getWriter();
			if (ctxWriter == writer) {
				logger.info(
						"Service is expected to have written the payload to the writer directly. Nothing is explicitly written to the writer by the service agent.");
				return;
			}
			throw new ApplicationError("Design Error: Service  " + ctx.getServiceName()
					+ " is designed to write payload directly to stream, but the writer is not assigned properly in the context.");
		}

		/*
		 * extract attachments if required
		 */
		if (this.attachmentFields != null) {
			InputData.storeFieldAttaches(this.attachmentFields, ctx, false);
		}

		if (this.attachmentColumns != null) {
			InputData.storeColumnAttaches(this.attachmentColumns, ctx, false);
		}

		if (this.responseTextFieldName != null) {
			/*
			 * service is supposed to have kept response ready for us
			 */
			Object obj = ctx.getObject(this.responseTextFieldName);
			if (obj == null) {
				obj = ctx.getValue(this.responseTextFieldName);
			}
			if (obj == null) {
				logger.info(this.responseTextFieldName
						+ " is expected to have a ready text as response. Service context has no value with that name.");
			} else {
				writer.setAsResponse(obj.toString());
			}
			return;
		}

		if (this.justOutputEveryThing) {
			/*
			 * there may be hierarchical data
			 */
			if (this.outputRecords != null) {
				for (OutputRecord rec : this.outputRecords) {
					rec.getReadyForOutput(ctx);
				}
			}
			/*
			 * it is up to the writer to pick data
			 */
			writer.pullDataFromContext(ctx);
			return;
		}
		/*
		 * messages
		 */

		List<FormattedMessage> msgs = ctx.getMessages();
		if (msgs.isEmpty() == false) {
			FormattedMessage[] messages = msgs.toArray(new FormattedMessage[0]);
			DataSheet sheet = FormattedMessage.toDataSheet(messages);
			OutputRecord.writeSheet(writer, ServiceProtocol.MESSAGES, sheet);
		}

		if (this.fieldNames != null) {
			this.writeFields(writer, ctx);
		}

		if (this.outputFields != null) {
			for (OutputField field : this.outputFields) {
				field.write(writer, ctx);
			}
		}

		if (this.dataSheets != null) {
			this.writeSheets(writer, ctx);
		}

		if (this.arrayNames != null) {
			this.writeArrays(writer, ctx);
		}

		if (this.objectNames != null) {
			this.writeObjects(writer, ctx);
		}

		if (this.outputRecords != null) {
			for (OutputRecord rec : this.outputRecords) {
				rec.write(writer, ctx);
			}
		}
	}

	private void writeFields(ResponseWriter writer, ServiceContext ctx) {
		for (String fieldName : this.fieldNames) {
			fieldName = TextUtil.getFieldValue(ctx, fieldName).toText();
			Value value = ctx.getValue(fieldName);
			if (value != null) {
				if (value.isUnknown() == false) {
					writer.setField(fieldName, value);
				}
				continue;
			}
			Object obj = ctx.getObject(fieldName);
			if (obj != null) {
				writer.setObject(fieldName, obj);
			}
		}
	}

	private void writeSheets(ResponseWriter writer, ServiceContext ctx) {
		for (String sheetName : this.dataSheets) {
			sheetName = TextUtil.getFieldValue(ctx, sheetName).toString();
			DataSheet sheet = ctx.getDataSheet(sheetName);
			if (sheet == null) {
				logger.info("Service context has no sheet with name {} for output.", sheetName);
			} else {
				OutputRecord.writeSheet(writer, sheetName, sheet);
			}
		}
	}

	private void writeArrays(ResponseWriter writer, ServiceContext ctx) {
		for (String arrayName : this.arrayNames) {
			DataSheet sheet = ctx.getDataSheet(arrayName);
			if (sheet != null) {
				writer.setArray(arrayName, sheet);
				continue;
			}

			Value value = ctx.getValue(arrayName);
			if (value != null) {
				Object[] arr = { value };
				writer.setArray(arrayName, arr);
			} else {
				logger.info("Service context has no sheet with name " + arrayName + " for output.");
			}

		}
	}

	private void writeObjects(ResponseWriter writer, ServiceContext ctx) {
		for (String objectName : this.objectNames) {
			Object obj = ctx.getObject(objectName);
			if (obj != null) {
				writer.setObject(objectName, obj);
				continue;
			}
		}
	}

	/**
	 * set output fields
	 *
	 * @param fields
	 */

	public void setOutputFields(String[] fields) {
		this.fieldNames = fields;
	}

	/**
	 * prepare data, possibly for writer to pull
	 *
	 * @param ctx
	 */
	public void prepareForOutput(ServiceContext ctx) {
		/*
		 * there may be hierarchical data
		 */
		if (this.outputRecords != null) {
			for (OutputRecord rec : this.outputRecords) {
				rec.getReadyForOutput(ctx);
			}
		}
	}

	/**
	 * when the service is being executed in the same jvm, then data is copied
	 * using this spec
	 *
	 * @param fromCtx
	 * @param toCtx
	 */
	public void copy(ServiceContext fromCtx, ServiceContext toCtx) {
		if (this.justOutputEveryThing) {
			toCtx.copyFrom(fromCtx);
			return;
		}

		if (this.fieldNames != null) {
			for (String fieldName : this.fieldNames) {
				Value value = fromCtx.getValue(fieldName);
				if (Value.isNull(value) == false) {
					toCtx.setValue(fieldName, value);
				}
			}
		}

		if (this.dataSheets != null) {
			for (String sheetName : this.dataSheets) {
				DataSheet sheet = fromCtx.getDataSheet(sheetName);
				if (sheet != null) {
					toCtx.putDataSheet(sheetName, sheet);
				}
			}
		}

		if (this.arrayNames != null) {
			for (String sheetName : this.arrayNames) {
				DataSheet sheet = fromCtx.getDataSheet(sheetName);
				if (sheet != null) {
					toCtx.putDataSheet(sheetName, sheet);
				}
			}
		}

		if (this.outputRecords != null) {
			for (OutputRecord rec : this.outputRecords) {
				rec.copy(fromCtx, toCtx);
			}
		}

	}

}
