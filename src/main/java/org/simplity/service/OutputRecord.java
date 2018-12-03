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

import org.simplity.json.JSONArray;
import org.simplity.json.JSONFields;
import org.simplity.json.JSONObject;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.data.MultiRowsSheet;
import org.simplity.kernel.dm.Field;
import org.simplity.kernel.dm.FieldType;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.sa.ResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * represents a record/row/table that is used as input/output of a service
 *
 */
public class OutputRecord {
	private static final Logger logger = LoggerFactory.getLogger(OutputRecord.class);

	/**
	 * name of this record/sheet/object. if null, then the data is the current
	 * object, typically service context
	 */
	String name;

	/**
	 * name to be used to output this sheet/object. if outputAs=FIELDS, this is
	 * ignored. Does NOT default to name, because we want to proide flexibility
	 * of keeping it null when name is not null
	 */
	String externalName;

	/**
	 * if null, then data in the source is sent as it is. if non-null fields
	 * from this record are used to pick-up data from the data source.
	 */
	String recordName;
	/**
	 * you may want to specify record for sake of documentation or for testing,
	 * but may want t avoid the over-head of data picking if the source already
	 * in the right format. Use this option for that.
	 */
	boolean recordIsForDocumentation;
	/**
	 * if this sheet has child rows for a parent. This name should match name
	 * attribute of the parent record. relevant only when outputAs=Object or
	 * Array
	 */
	String parentSheetName;

	/**
	 * required when parentSheetName is specified. One or more key field names
	 * in this sheet that is used to link it with parent sheet.
	 */
	String[] linkFieldsInThisSheet;
	/**
	 * required when parentSheetName is specified. key/s in the parent sheet
	 * used for linking. Must have the key/s in the same order as in
	 * linkFieldsInThisSheet.
	 */
	String[] linkFieldsInParentSheet;
	/**
	 * when name is non-null, source of data for this record could be either an
	 * object, or a data sheet. set this to true to use object, false for data
	 * sheet.
	 */
	boolean sourceIsAnObject;

	/**
	 * how is the data expected by the client. Useful in dealing with external
	 * client who may choose different data structure than the one used by
	 * server. Different combinations of source and out put are possible.
	 */
	DataStructureType outputAs = DataStructureType.FIELDS;
	/*
	 * we cache child records for convenience
	 */
	private OutputRecord[] childRecords;

	private Field[] fields;

	private boolean isComplexStructure;

	/**
	 * default constructor
	 */
	public OutputRecord() {
		// default
	}

	/**
	 * used by utility programs that create this programmatically
	 *
	 * @param name
	 *            attribute name with which data is to be picked-up for this.
	 *            null if fields are nothing but attributes in the current
	 *            source
	 *            object
	 * @param externalName
	 *            name with which data from the record is to be output. not
	 *            relevant if outputAs="fields
	 * @param recordName
	 *            null if data from the source to be sent as it is. non-null to
	 *            provide list of fields to be output
	 * @param sourceIsAnObject
	 *            relevant if name is non-null. true if data is an object, false
	 *            if it is a data sheet
	 * @param outputAs
	 */
	public OutputRecord(String name, String externalName, String recordName, boolean sourceIsAnObject,
			DataStructureType outputAs) {
		this.name = name;
		this.externalName = externalName;
		this.recordName = recordName;
		this.sourceIsAnObject = sourceIsAnObject;
		this.outputAs = outputAs;
	}

	/**
	 * create output record for a child sheet
	 *
	 * @param parentSheet
	 *            name of outputRecord to be used as parent of this record
	 * @param childColNames
	 *            key field names in this record to be used to link it with
	 *            parent
	 * @param parentColNames
	 *            key field names in the parent record to be used for linking
	 */
	public void linkToParent(String parentSheet, String[] childColNames, String[] parentColNames) {
		this.parentSheetName = parentSheet;
		this.linkFieldsInThisSheet = childColNames;
		this.linkFieldsInParentSheet = parentColNames;
	}

	/**
	 * @param recordName
	 *            the recordName to set
	 */
	public void setRecordName(String recordName) {
		this.recordName = recordName;
	}

	/**
	 * set child records
	 *
	 * @param children
	 *            array that may have nulls at the end
	 * @param nbrChildren
	 *            number of non-null entries in this
	 */
	void setChildren(OutputRecord[] children) {
		this.childRecords = children;
	}

	/**
	 * open shop and get ready for service
	 *
	 */
	public void getReady() {
		if (this.recordName == null) {
			if (this.name != null) {
				return;
			}
			throw new ApplicationError("Output record should have either sheet name or record name specified.");
		}
		if (this.recordIsForDocumentation) {
			return;
		}
		Record record = ComponentManager.getRecord(this.recordName);
		this.fields = record.getFields();
		if (this.fields == null) {
			logger.info("Record " + this.recordName + " yielded no fields");
		} else {
			this.isComplexStructure = record.isComplexStruct();
		}

	}

	/**
	 * @param ctx
	 * @return
	 */
	int validate(ValidationContext ctx) {
		return 0;
	}

	/**
	 * write data for this record
	 *
	 * @param writer
	 * @param ctx
	 */
	void write(ResponseWriter writer, ServiceContext ctx) {
		if (this.parentSheetName != null) {
			logger.info("Output record {} will be written out later as part of its parent {}", this.name,
					this.parentSheetName);
			return;
		}
		/*
		 * this appears quite complex because we provide flexibility. Tricky
		 * part is to ensure that we cover all possible combinations. After
		 * several trials and difficulties, we have come to this way of looking
		 * at the problem
		 *
		 *
		 * 1. data source has 4 possibilities. root-object(this.name = null),
		 * JsonObject, jsonArray or dataSheet
		 *
		 * 2. output-type has 4 options. Fields, object, array, sheet.
		 *
		 * 3. data-content has 2 options - this.fields == null implying that the
		 * data from the source is to be output as it is, or this.fields != null
		 * when these fields are to be used to select/filter data to be
		 * written-out
		 *
		 *
		 * So, in all we have to handle 4 * 4 * 2 = 32 combinations.
		 *
		 * slicing/dicing is tricky because modularity of the code varies
		 * drastically. We gave primary importance to readability and came up
		 * with this way of dividing the tasks
		 *
		 * a. We define sheet, array, and json. At most one of these three is
		 * non-null
		 * (3 cases) and all null (4th case when name=null)
		 *
		 * b. we define 8 methods at top level. We expect each of these 8
		 * methods to take care of four possibilities to cover all 32
		 * possibilities
		 */
		JSONObject json = null;
		DataSheet sheet = null;
		JSONArray arr = null;
		if (this.name != null) {
			if (this.sourceIsAnObject) {
				Object obj = ctx.getObject(this.name);
				if (obj == null) {
					logger.info("No object named {} found in context. Data not written out", this.name);
					return;
				}
				if (obj instanceof JSONObject) {
					json = (JSONObject) obj;
				} else if (obj instanceof JSONArray) {
					arr = (JSONArray) obj;
				} else {
					logger.error("We expected a JSONObject/JSONArray named {}, but found {}. Data not written out",
							this.name, obj.getClass().getName());
					return;
				}
			} else {
				sheet = ctx.getDataSheet(this.name);
				if (sheet == null) {
					logger.info("Data sheet named {} not found in context. Data not written out", this.name);
					return;
				}
			}
		}
		/*
		 * at this point, we have ensured that either this.name==null or one and
		 * only one of(json, arr, sheet) is non-null
		 */
		if (this.fields == null) {
			switch (this.outputAs) {
			case FIELDS:
				this.writeFieldsWithNoSpec(writer, json, arr, sheet, ctx);
				return;

			case OBJECT:
				this.writeObjectWithNoSpec(writer, json, arr, sheet, ctx);
				return;

			case ARRAY:
				this.writeArrayWithNoSpec(writer, json, arr, sheet, ctx);
				return;
			case SHEET:
				this.writeSheetWithNoSpec(writer, json, arr, sheet, ctx);
				return;
			default:
				this.logNoData();
			}
		} else {
			switch (this.outputAs) {
			case FIELDS:
				this.writeFieldsWithSpec(writer, json, arr, sheet, ctx);
				return;

			case OBJECT:
				this.writeObjectWithSpec(writer, json, arr, sheet, ctx);
				return;

			case ARRAY:
				this.writeArrayWithSpec(writer, json, arr, sheet, ctx);
				return;
			case SHEET:
				this.writeSheetWithSpec(writer, json, arr, sheet, ctx);
				return;
			default:
				this.logNoData();
			}
		}
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private boolean writeFieldsWithNoSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		if (json != null) {
			this.writeJsonFields(writer, json);
		} else if (arr != null) {
			JSONObject j = arr.optJSONObject(0);
			if (j != null) {
				this.writeJsonFields(writer, j);
			}
		} else if (sheet != null) {
			this.writeSheetFields(writer, sheet);
		} else {
			this.logNoData();
			return false;
		}
		return true;
	}

	private void writeJsonFields(ResponseWriter writer, JSONObject json) {
		for (String key : json.keySet()) {
			writer.setField(key, json.opt(key));
		}
	}

	private void writeSheetFields(ResponseWriter writer, DataSheet sheet) {
		for (String key : sheet.getColumnNames()) {
			writer.setField(key, sheet.getValue(key));
		}
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeObjectWithNoSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		writer.beginObject(this.externalName);
		boolean ok = this.writeFieldsWithNoSpec(writer, json, arr, sheet, ctx);
		if (ok && this.childRecords != null) {
			FieldsCollection values = this.getCurrentValues(json, arr, sheet, ctx);
			this.writeChildren(writer, values, ctx);
		}
		writer.endObject();
		return;
	}

	private FieldsCollection getCurrentValues(JSONObject json, JSONArray arr, DataSheet sheet, ServiceContext ctx) {
		if (sheet != null) {
			return sheet;
		}
		if (json != null) {
			return new JSONFields(json);
		}
		if (arr != null) {
			return new JSONFields(arr.getJSONObject(0));
		}
		return ctx;
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeArrayWithNoSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		writer.beginArray(this.externalName);
		int nbr = 0;
		if (json != null) {
			this.writeJsonObject(writer, json, ctx);
		} else if (arr != null) {
			nbr = arr.length();
			for (int i = 0; i < nbr; i++) {
				JSONObject j = arr.optJSONObject(i);
				if (j != null) {
					this.writeJsonObject(writer, j, ctx);
				}
			}
		} else if (sheet != null) {
			nbr = sheet.length();
			String[] keys = sheet.getColumnNames();
			for (int i = 0; i < nbr; i++) {
				this.writeFieldsObject(writer, keys, sheet.getRowAsFields(i), ctx);
			}
		} else {
			this.logNoData();
		}
		writer.endArray();
		return;
	}

	private void writeJsonObject(ResponseWriter writer, JSONObject json, ServiceContext ctx) {
		writer.beginObjectAsArrayElement();
		this.writeJsonFields(writer, json);
		if (this.childRecords != null) {
			this.writeChildren(writer, new JSONFields(json), ctx);
		}
		writer.endObject();
	}

	private void writeFieldsObject(ResponseWriter writer, String[] keys, FieldsCollection values, ServiceContext ctx) {
		writer.beginObjectAsArrayElement();
		writeFields(writer, keys, values);
		if (this.childRecords != null) {
			this.writeChildren(writer, values, ctx);
		}
		writer.endObject();
	}

	/**
	 * @param writer
	 * @param keys
	 * @param rowAsFieldsCollection
	 */
	static private void writeFields(ResponseWriter writer, String[] keys, FieldsCollection values) {
		for (String key : keys) {
			writer.setField(key, values.getValue(key));
		}
	}

	private void logNoData() {
		logger.error("Reord has no fields and no name. No output possible");
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeSheetWithNoSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		writer.beginArray(this.externalName);
		if (json != null) {
			String[] keys = json.keySet().toArray(new String[0]);
			this.writeHeader(writer, keys);
			this.writeDataRow(writer, keys, new JSONFields(json));
		} else if (arr != null) {
			this.writeArrayAsRows(writer, arr);
		} else if (sheet != null) {
			this.writeSheetAsRows(writer, sheet);
		} else {
			this.logNoData();
		}
		writer.endArray();
		return;
	}

	private void writeHeader(ResponseWriter writer, String[] keys) {
		writer.beginArrayAsArrayElement();
		for (String key : keys) {
			writer.addToArray(key);
		}
		writer.endArray();
	}

	private void writeDataRow(ResponseWriter writer, String[] keys, FieldsCollection values) {
		writer.beginArrayAsArrayElement();
		for (String key : keys) {
			writer.addToArray(values.getValue(key));
		}
		writer.endArray();
	}

	private void writeArrayAsRows(ResponseWriter writer, JSONArray arr) {
		JSONObject json = arr.optJSONObject(0);
		if (json == null) {
			return;
		}
		String[] keys = json.keySet().toArray(new String[0]);
		this.writeHeader(writer, keys);
		int nbr = arr.length();
		for (int i = 0; i < nbr; i++) {
			json = arr.optJSONObject(i);
			if (json != null) {
				this.writeDataRow(writer, keys, new JSONFields(json));
			}
		}
	}

	private void writeSheetAsRows(ResponseWriter writer, DataSheet sheet) {
		int nbr = sheet.length();
		String[] keys = sheet.getColumnNames();
		this.writeHeader(writer, keys);
		for (int i = 0; i < nbr; i++) {
			writer.beginArrayAsArrayElement();
			Value[] values = sheet.getRow(i);
			for (int j = 0; j < keys.length; j++) {
				writer.addToArray(values[j]);
			}
			writer.endArray();
		}
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeFieldsWithSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		if (json != null) {
			this.writeOurFields(writer, new JSONFields(json), ctx);
		} else if (arr != null) {
			JSONObject j = arr.optJSONObject(0);
			if (j != null) {
				this.writeOurFields(writer, new JSONFields(j), ctx);
			}
		} else if (sheet != null) {
			this.writeOurFields(writer, sheet, ctx);
		} else {
			this.writeOurFields(writer, ctx, ctx);
		}
	}

	private void writeOurFields(ResponseWriter writer, FieldsCollection values, ServiceContext ctx) {
		if (this.isComplexStructure) {
			this.writeComplexStructure(writer, values, ctx);
			return;
		}
		for (Field field : this.fields) {
			writer.setField(field.getExternalName(), values.getValue(field.getName()));
		}
	}

	/**
	 * write sub-objects based on non-primitive fields in this record
	 *
	 * @param writer
	 * @param values
	 * @param ctx
	 */
	private void writeComplexStructure(ResponseWriter writer, FieldsCollection values, ServiceContext ctx) {
		JSONObject parentJson = null;
		if (values instanceof JSONFields) {
			parentJson = ((JSONFields) values).getJson();
		} else if (values instanceof ServiceContext == false) {
			throw new ApplicationError("Complex structure can only be read from a JSON object or from service context");
		}
		for (Field field : this.fields) {
			if (field.isPrimitive()) {
				writer.setField(field.getExternalName(), values.getValue(field.getName()));
				continue;
			}
			/*
			 * child is not primitive. We have to delegate it to a child outputRecord
			 */
			FieldType ft = field.getFieldType();
			String fieldName = field.getName();
			if (ft == FieldType.RECORD) {
				this.writeComplexChildObject(writer, field, parentJson, ctx);
				continue;
			}

			if (ft == FieldType.RECORD_ARRAY) {
				this.writeComplexChildArray(writer, field, parentJson, ctx);
				continue;
			}

			Object obj = parentJson == null ? ctx.getObject(fieldName) : parentJson.opt(fieldName);
			if (obj == null || obj instanceof JSONArray == false) {
				logger.info("Child array named {}not found in parentObject/serviceContext", fieldName);
				continue;
			}

			JSONArray arr = (JSONArray) obj;
			OutputRecord child = field.getOutputRecord(DataStructureType.ARRAY);
			/*
			 * array of values
			 */
			child.writeArrayOfValues(writer, arr);
		}
	}

	private void writeComplexChildObject(ResponseWriter writer, Field field, JSONObject parentJson,
			ServiceContext ctx) {
		OutputRecord child = field.getOutputRecord(DataStructureType.OBJECT);
		String fieldName = field.getName();
		if (parentJson != null) {
			JSONObject json = parentJson.optJSONObject(fieldName);
			if (json == null) {
				logger.info("No child json found fr attribute {}. Child object is not written.", fieldName);
			} else {
				child.writeObjectWithSpec(writer, json, null, null, ctx);
			}
			return;
		}
		Object obj = ctx.getObject(fieldName);
		if (obj != null && obj instanceof JSONObject) {
			child.writeObjectWithSpec(writer, (JSONObject) obj, null, null, ctx);
			return;
		}

		DataSheet sheet = ctx.getDataSheet(fieldName);
		if (sheet == null) {
			logger.info("NO json or data sheet found in context with name {}. Child object not written", fieldName);
		} else {
			child.writeObjectWithSpec(writer, null, null, sheet, ctx);
		}
	}

	private void writeComplexChildArray(ResponseWriter writer, Field field, JSONObject parentJson, ServiceContext ctx) {
		OutputRecord child = field.getOutputRecord(DataStructureType.ARRAY);
		String fieldName = field.getName();
		if (parentJson != null) {
			JSONArray arr = parentJson.optJSONArray(fieldName);
			if (arr == null) {
				logger.info("No child array found for attribute {}. Child array is not written.", fieldName);
			} else {
				child.writeArrayFromArray(writer, arr, ctx);
			}
			return;
		}
		Object obj = ctx.getObject(fieldName);
		if (obj != null && obj instanceof JSONArray) {
			child.writeArrayFromArray(writer, (JSONArray) obj, ctx);
			return;
		}

		DataSheet sheet = ctx.getDataSheet(fieldName);
		if (sheet == null) {
			logger.info("NO jsonarray or data sheet found in context with name {}. Child array not written", fieldName);
		} else {
			child.writeArrayFromSheet(writer, sheet, ctx);
		}
	}

	private void writeArrayOfValues(ResponseWriter writer, JSONArray arr) {
		writer.beginArray(this.externalName);
		int nbr = arr.length();
		for (int i = 0; i < nbr; i++) {
			writer.addToArray(arr.opt(i));
		}
		writer.endArray();
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeObjectWithSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		writer.beginObject(this.externalName);
		this.writeFieldsWithSpec(writer, json, arr, sheet, ctx);
		if (this.childRecords != null) {
			FieldsCollection values = this.getCurrentValues(json, arr, sheet, ctx);
			this.writeChildren(writer, values, ctx);
		}
		writer.endObject();
	}

	/**
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeArrayWithSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		if (arr != null) {
			this.writeArrayFromArray(writer, arr, ctx);
			return;
		}
		if (sheet != null) {
			this.writeArrayFromSheet(writer, sheet, ctx);
			return;
		}

		writer.beginArray(this.externalName);
		if (json != null) {
			this.writeOurFieldsAsObject(writer, new JSONFields(json), ctx);
		} else {
			this.writeOurFieldsAsObject(writer, ctx, ctx);
		}
		writer.endArray();
		return;
	}

	/**
	 * source and destination are array of objects. This method is also directly
	 * invoked on a child record from its parent
	 *
	 * @param writer
	 * @param arr
	 * @param ctx
	 */
	private void writeArrayFromArray(ResponseWriter writer, JSONArray arr, ServiceContext ctx) {
		writer.beginArray(this.externalName);
		int nbr = arr.length();
		for (int i = 0; i < nbr; i++) {
			JSONObject json = arr.optJSONObject(i);
			if (json != null) {
				this.writeOurFieldsAsObject(writer, new JSONFields(json), ctx);
			}
		}
		writer.endArray();
	}

	/**
	 * source is data sheet destination is array of objects. This method is also
	 * directly
	 * invoked on a child record from its parent
	 *
	 * @param writer
	 * @param arr
	 * @param ctx
	 */
	private void writeArrayFromSheet(ResponseWriter writer, DataSheet sheet, ServiceContext ctx) {
		writer.beginArray(this.externalName);
		int nbr = sheet.length();
		for (int i = 0; i < nbr; i++) {
			this.writeOurFieldsAsObject(writer, sheet.getRowAsFields(i), ctx);
		}
		writer.endArray();
	}

	private void writeOurFieldsAsObject(ResponseWriter writer, FieldsCollection values, ServiceContext ctx) {
		writer.beginObjectAsArrayElement();
		this.writeOurFields(writer, values, ctx);
		if (this.childRecords != null) {
			this.writeChildren(writer, values, ctx);
		}
		writer.endObject();
	}

	/**
	 * sheet is an array of arrays. first row is the header row, and subsequent
	 * rows are data rows
	 *
	 * @param writer
	 * @param json
	 * @param arr
	 * @param sheet
	 * @param ctx
	 */
	private void writeSheetWithSpec(ResponseWriter writer, JSONObject json, JSONArray arr, DataSheet sheet,
			ServiceContext ctx) {
		writer.beginArray(this.externalName);
		this.writeOurHeader(writer);
		int nbr = 0;
		if (json != null) {
			/*
			 * one data row for this json
			 */
			this.writeOurDataRow(writer, new JSONFields(json));
		} else if (arr != null) {
			for (int i = 0; i < nbr; i++) {
				JSONObject j = arr.optJSONObject(i);
				if (j != null) {
					this.writeOurDataRow(writer, new JSONFields(j));
				}
			}
		} else if (sheet != null) {
			nbr = sheet.length();
			for (int i = 0; i < nbr; i++) {
				this.writeOurDataRow(writer, sheet.getRowAsFields(i));
			}
		} else {
			this.writeOurDataRow(writer, ctx);
		}
		writer.endArray();
		return;
	}

	private void writeOurHeader(ResponseWriter writer) {
		writer.beginArrayAsArrayElement();
		for (Field field : this.fields) {
			writer.addToArray(field.getExternalName());
		}
		writer.endArray();
	}

	private void writeOurDataRow(ResponseWriter writer, FieldsCollection values) {
		writer.beginArrayAsArrayElement();
		for (Field field : this.fields) {
			writer.addToArray(values.getValue(field.getName()));
		}
		writer.endArray();
	}

	/*
	 * methods to convert parent-child data sheets into JSON objects
	 */
	/**
	 * called once by OutputData to get ready to write()
	 *
	 * @param ctx
	 */
	void getReadyForOutput(ServiceContext ctx) {
		if (this.parentSheetName == null) {
			return;
		}
		DataSheet sheet = ctx.getDataSheet(this.name);
		if (sheet == null) {
			logger.info("No data sheet available for child {}", this.name);
			return;
		}
		/*
		 * we create one data sheet per unique key-values and save them back in
		 * ctx.dataSheets with codified keys.
		 */
		String[] columnNames = sheet.getColumnNames();
		ValueType[] valueTypes = sheet.getValueTypes();
		int[] keyIndexes = this.getKeyIndexes(sheet);
		int nbrRows = sheet.length();

		for (int i = 0; i < nbrRows; i++) {
			Value[] row = sheet.getRow(i);
			String key = this.createKeyString(row, keyIndexes);
			DataSheet child = ctx.getDataSheet(key);
			if (child == null) {
				child = new MultiRowsSheet(columnNames, valueTypes);
				ctx.putDataSheet(key, child);
			}
			child.addRow(row);
		}
	}

	/**
	 * write rows for child sheets
	 *
	 * @param writer
	 * @param values
	 *            of this parent row. passed down for the child to get parent
	 *            key values
	 * @param ctx
	 */
	private void writeChildren(ResponseWriter writer, FieldsCollection values, ServiceContext ctx) {
		for (OutputRecord child : this.childRecords) {
			child.writeAsChildSheet(writer, values, ctx);
		}
	}

	/**
	 * write an array of child rows as an attribute of the current object
	 *
	 * @param writer
	 * @param parentKeyValues
	 */
	private void writeAsChildSheet(ResponseWriter writer, FieldsCollection parentValues, ServiceContext ctx) {
		String key = this.createParentKeyString(parentValues);
		DataSheet sheet = ctx.getDataSheet(key);
		if (sheet == null) {
			logger.info("No child rows for sheet {} with key {}", this.name, key);
			return;
		}
		if (this.fields == null) {
			this.writeArrayWithNoSpec(writer, null, null, sheet, ctx);
		} else {
			this.writeArrayWithSpec(writer, null, null, sheet, ctx);
		}
	}

	/**
	 * Synthesize key as name+'.'+keyValue1{+'.'+keyValue2}...
	 *
	 * @param values
	 *            source of values for keys
	 * @return unique key for that row and keys
	 */
	private String createParentKeyString(FieldsCollection values) {
		StringBuilder sbf = new StringBuilder(this.name);
		for (String key : this.linkFieldsInParentSheet) {
			sbf.append('.').append(values.getValue(key));
		}
		return sbf.toString();
	}

	/**
	 * Synthesize key as name+'.'+keyValue1{+'.'+keyValue2}...
	 *
	 * @param values
	 *            source of values for keys
	 * @return unique key for that row and keys
	 */
	private String createKeyString(Value[] row, int[] indexes) {
		StringBuilder sbf = new StringBuilder(this.name);
		for (int i : indexes) {
			sbf.append('.').append(row[indexes[i]].toString());
		}
		return sbf.toString();
	}

	private int[] getKeyIndexes(DataSheet sheet) {
		int nbr = this.linkFieldsInThisSheet.length;
		int[] result = new int[nbr];
		for (int i = 0; i < nbr; i++) {
			result[i] = sheet.getColIdx(this.linkFieldsInThisSheet[i]);
		}
		return result;
	}

	/**
	 * @param fromCtx
	 * @param toCtx
	 */
	void copy(ServiceContext fromCtx, ServiceContext toCtx) {
		if (this.name == null) {
			if (this.fields != null) {
				for (Field field : this.fields) {
					String fieldName = field.getName();
					toCtx.setValue(fieldName, fromCtx.getValue(fieldName));
				}
			}
			return;
		}
		if (this.sourceIsAnObject) {
			toCtx.setObject(this.name, fromCtx.getObject(this.name));
		} else {
			toCtx.putDataSheet(this.name, fromCtx.getDataSheet(this.name));
		}
	}

	/**
	 * write out a data sheet as array of objects
	 *
	 * @param writer
	 * @param sheetName
	 * @param sheet
	 */
	public static void writeSheet(ResponseWriter writer, String sheetName, DataSheet sheet) {
		writer.beginArray(sheetName);
		int nbr = sheet.length();
		String[] keys = sheet.getColumnNames();
		for (int i = 0; i < nbr; i++) {
			writer.beginObjectAsArrayElement();
			writeFields(writer, keys, sheet.getRowAsFields(i));
			writer.endObject();
		}
		writer.endArray();
	}
}
