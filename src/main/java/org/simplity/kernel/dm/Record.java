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
package org.simplity.kernel.dm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.simplity.json.JSONArray;
import org.simplity.json.JSONException;
import org.simplity.json.JSONObject;
import org.simplity.json.JSONWriter;
import org.simplity.kernel.AppDataCacherInterface;
import org.simplity.kernel.Application;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.FilterCondition;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.Messages;
import org.simplity.kernel.comp.Component;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.comp.ValidationUtil;
import org.simplity.kernel.data.DataPurpose;
import org.simplity.kernel.data.DataSerializationType;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.data.FlatFileRowType;
import org.simplity.kernel.data.MultiRowsSheet;
import org.simplity.kernel.data.SingleRowSheet;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.dt.DataType;
import org.simplity.kernel.dt.DataTypeSuggester;
import org.simplity.kernel.util.JsonUtil;
import org.simplity.kernel.util.TextUtil;
import org.simplity.kernel.util.XmlUtil;
import org.simplity.kernel.value.BooleanValue;
import org.simplity.kernel.value.IntegerValue;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.sa.ResponseWriter;
import org.simplity.service.DataStructureType;
import org.simplity.service.InputRecord;
import org.simplity.service.OutputRecord;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceProtocol;
import org.simplity.tp.RelatedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main part of our data model. Every piece of data that the
 * application has to keep as part of "system of records" must be modeled into
 * this. Data structures that are used as input to, or output from a service are
 * modeled as records as well.
 *
 * <p>
 * It is common industry practice to have "physical data model" and "logical
 * data model" for proper understanding. We encourage such designs before
 * recording them using the Record concept.
 *
 * <p>
 * Record is a good candidate to represent any data structure that is used
 * across components, even if it is not persisted.
 */
public class Record implements Component {

	private static final Logger logger = LoggerFactory.getLogger(Record.class);

	/** header row of returned sheet when there is only one column */
	private static String[] SINGLE_HEADER = { "value" };
	/** header row of returned sheet when there are two columns */
	private static String[] DOUBLE_HEADER = { "key", "value" };

	private static final char COMMA = ',';
	private static final char PARAM = '?';
	private static final char EQUAL = '=';
	private static final String EQUAL_PARAM = "=?";
	private static final char PERCENT = '%';
	private static final String TABLE_ACTION_FIELD_NAME = ServiceProtocol.TABLE_ACTION_FIELD_NAME;

	/*
	 * initialization deferred because it needs bootstrapping..
	 */
	private static Field TABLE_ACTION_FIELD = null;

	private static final ComponentType MY_TYPE = ComponentType.REC;

	/*
	 * oracle sequence name is generally tableName_SEQ.
	 */
	private static final String DEFAULT_SEQ_SUFFIX = "_SEQ.NEXTVAL";

	private static final char KEY_JOINER = 0;

	private static final String KEY_PREFIX = "rec.";

	/** * Name of this record/entity, as used in application */
	@FieldMetaData(isRequired = true)
	String name;

	/**
	 * module name + name would be unique for a component type within an
	 * application. we also insist on a java-like convention that the the
	 * resource is stored in a folder structure that mimics module name
	 */
	String moduleName;
	/** type of this record */
	RecordUsageType recordType = RecordUsageType.STORAGE;
	/**
	 * name of the rdbms table, if this is either a storage table, or a view
	 * that is to be defined in the rdbms
	 */
	String tableName;

	/**
	 * has this table got an internal key, and do you want it to be managed
	 * automatically?
	 */
	@FieldMetaData(leaderField = "tableName")
	boolean keyToBeGenerated;
	/**
	 * oracle does not support auto-increment. Standard practice is to have a
	 * sequence, typically named as tableName_SEQ, and use sequence.NEXTVAL as
	 * value of the key field. If you follow different standard that that,
	 * please specify the expression. We have made this an expression to provide
	 * flexibility to have any expression, including functions that you may have
	 * written.
	 */
	@FieldMetaData(leaderField = "tableName")
	String sequenceName;
	/**
	 * if this table is expected to have large number of rows, we would like to
	 * protect against a select with no where conditions. Of course one can
	 * always argue that this is no protection, as some one can easily put a
	 * condition like 1 = 1
	 */
	boolean okToSelectAll;

	/**
	 * child records that are to be read whenever a row from this record is
	 * read.
	 */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.REC)
	String[] childrenToBeRead = null;
	/**
	 * child records to be saved along with this record. operations for this
	 * record
	 */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.REC)
	String[] childrenToBeSaved = null;
	/** fields that this record is made-up of */
	@FieldMetaData(isRequired = true)
	Field[] fields = new Field[0];

	/**
	 * In case this is a table that supplies key-value list for drop-downs, then
	 * we use primary key as internal key. Specify the field to be used as
	 * display value. key of this table is the internal value
	 */
	String listFieldName = null;

	/**
	 * relevant only if valueListFieldName is used. If the list of values need
	 * be further filtered with a key, like country code for list of state,
	 * specify the that field name.
	 */
	@FieldMetaData(leaderField = "listFieldName")
	String listGroupKeyName = null;

	/**
	 * what is the sheet name to be used as input/output sheet. (specifically
	 * used in creating services on the fly)
	 */
	String defaultSheetName = null;

	/**
	 * if this record is used for a suggestion service, field that is used for
	 * search
	 */
	String suggestionKeyName;
	/** what fields do we respond back with for a suggestion service */
	@FieldMetaData(leaderField = "suggestionKeyName")
	String[] suggestionOutputNames;
	/** is this record only for reading? */
	boolean readOnly;

	/**
	 * in case this is a view, then the record from which fields are referred by
	 * default
	 */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.REC)
	String defaultRefRecord;

	/**
	 * if this application uses multiple schemas, and the underlying table of
	 * this record belongs to a schema other than the default, then specify it
	 * here, so that the on-the-fly services based on this record can use the
	 * right schema.
	 */
	String schemaName;

	/**
	 * should we insist that the client returns the last time stamp during an
	 * update that we match with the current row before updating it? This
	 * technique allows us to detect whether the row was updated after it was
	 * sent to client.
	 */
	boolean useTimestampForConcurrency = false;

	/**
	 * if this table is (almost) static, and the vauleList that is delivered on
	 * a list request can be cached by the agent. Valid only if valueListField
	 * is set (list_ auto service is enabled) if valueListKey is specified, the
	 * result will be cached by that field. For example, by country-code.
	 */
	boolean okToCache;

	/**
	 * If this record represents a data structure corresponding to an object
	 * defined in the RDBMS, what is the Object name in the sql. This is used
	 * while handling stored procedure parameters that pass objects and array of
	 * objects
	 */
	String sqlStructName;

	/**
	 * do you intend to use this to read from /write to flat file with fixed
	 * width rows? If this is set to true, then each field must set fieldWidth;
	 */
	boolean forFixedWidthRow;

	/**
	 * if this record allows update of underlying table, and the table is
	 * "cacheable", then there may be one or more records that would have set
	 * okToCache="true". We may have to notify them in case data in this table
	 * changes.
	 */
	String[] recordsToBeNotifiedOnChange;
	/*
	 * following fields are assigned for caching/performance
	 */
	/**
	 * Is there at least one field that has validations linked to another field.
	 * Initialized during init operation
	 */
	private boolean hasInterFieldValidations;

	/*
	 * standard fields are cached
	 */
	private Field modifiedStampField;
	private Field modifiedUserField;
	private Field createdUserField;

	/** we need the set to validate field names at time */
	private final Map<String, Field> indexedFields = new HashMap<String, Field>();

	/** and field names of course. cached after loading */
	private String[] fieldNames;
	/** sql for reading a row for given primary key value */
	private String readSql;

	/** select f1,f2,..... WHERE used in filtering */
	private String filterSql;

	/** sql ready to insert a row into the table */
	private String insertSql;

	/** sql to update every field. (Not selective update) */
	private String updateSql;

	/**
	 * we skip few standard fields while updating s row. Keeping this count
	 * simplifies code
	 */
	private String deleteSql;

	/** sql to be used for a list action */
	private String listSql;
	/** value types of fields selected for list action */
	private ValueType[] valueListTypes;
	/** value type of key used in list action */
	private ValueType valueListKeyType;

	/** sql to be used for a suggestion action */
	private String suggestSql;

	/** sequence of oracle if required */
	private String sequence;

	/**
	 * This record is a dataObject if any of its field is non-primitive (array
	 * or child-record
	 */
	private boolean isComplexStruct;

	/** min length of record in case forFixedWidthRow = true */
	private int minRecordLength;

	/**
	 * in case the primary is a composite key : with more than one fields, then
	 * we keep all of them in an array. This is null if we have a single key. We
	 * have designed it that way to keep the single key case as simple as
	 * possible
	 */
	private Field[] allPrimaryKeys;

	/** parent key, in case parent has composite primary key */
	private Field[] allParentKeys;

	/** " WHERE key1=?,key2=? */
	private String primaryWhereClause;

	// private int nbrUpdateFields = 0;

	private int nbrInsertFields = 0;

	private Field[] encryptedFields;
	/*
	 * methods for ComponentInterface
	 */

	@Override
	public String getSimpleName() {
		return this.name;
	}

	/**
	 * @param fieldName
	 * @return field or null
	 */
	public Field getField(String fieldName) {
		return this.indexedFields.get(fieldName);
	}

	/**
	 * @param fieldName
	 * @return field or null
	 */
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < this.fieldNames.length; i++) {
			if (this.fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * to avoid confusion, we do not have a method called getName.
	 *
	 * @return qualified name of this record
	 */
	@Override
	public String getQualifiedName() {
		if (this.moduleName == null) {
			return this.name;
		}
		return this.moduleName + '.' + this.name;
	}

	/** @return array of primary key fields. */
	public Field[] getPrimaryKeyFields() {
		return this.allPrimaryKeys;
	}

	/** @return dependent children to be input for saving */
	public String[] getChildrenToInput() {
		return this.childrenToBeSaved;
	}

	/** @return dependent child records that are read along */
	public String[] getChildrenToOutput() {
		return this.childrenToBeRead;
	}

	/**
	 * get the default name of data sheet to be used for input/output
	 *
	 * @return sheet name
	 */
	public String getDefaultSheetName() {
		return this.defaultSheetName;
	}

	/**
	 * get value types of fields in the same order that getFieldName() return
	 * field names
	 *
	 * @return value types of fields
	 */
	public ValueType[] getValueTypes() {
		ValueType[] types = new ValueType[this.fieldNames.length];
		int i = 0;
		for (Field field : this.fields) {
			types[i] = field.getValueType();
			i++;
		}
		return types;
	}

	/**
	 * get value types of fields in the same order that getFieldName() and an
	 * additional field for save action
	 *
	 * @return value types of fields and the last field a save action, which is
	 *         TEXT
	 */
	private Field[] getFieldsWithSave() {
		int n = this.fields.length + 1;
		Field[] allFields = new Field[n];
		n = 0;
		for (Field field : this.fields) {
			allFields[n++] = field;
		}
		allFields[n] = TABLE_ACTION_FIELD;
		return allFields;
	}

	/**
	 * to avoid confusion, we do not have a method called setName. name is
	 * split, if required into module name and name
	 *
	 * @param nam
	 *            qualified name
	 */
	public void setQualifiedName(String nam) {
		int idx = nam.lastIndexOf('.');
		if (idx == -1) {
			this.name = nam;
			return;
		}
		this.moduleName = nam.substring(0, idx);
		this.name = nam.substring(idx + 1);
	}

	/**
	 * @param moduleName
	 */
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	/**
	 * @param tableName
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * create an empty data sheet based on this record
	 *
	 * @param forSingleRow
	 *            true if you intend to store only one row. False otherwise
	 * @param addActionColumn
	 *            should we add a column to have action to be performed on each
	 *            row
	 * @return an empty sheet ready to receive data
	 */
	public DataSheet createSheet(boolean forSingleRow, boolean addActionColumn) {
		Field[] sheetFeilds = this.fields;
		if (addActionColumn) {
			sheetFeilds = this.getFieldsWithSave();
		}
		if (forSingleRow) {
			return new SingleRowSheet(sheetFeilds);
		}
		return new MultiRowsSheet(sheetFeilds);
	}

	/**
	 * create an empty data sheet based on this record for a subset of fields
	 *
	 * @param colNames
	 *            subset of column names to be included
	 * @param forSingleRow
	 *            true if you intend to use this to have just one row
	 * @param addActionColumn
	 *            should we add a column to have action to be performed on each
	 *            row
	 * @return an empty sheet ready to receive rows of data
	 */
	public DataSheet createSheet(String[] colNames, boolean forSingleRow, boolean addActionColumn) {
		Field[] subset = new Field[colNames.length];
		int i = 0;
		for (String colName : colNames) {
			Field field = this.indexedFields.get(colName);
			if (field == null) {
				throw new ApplicationError("Record " + this.getQualifiedName() + " has no field named " + colName);
			}
			subset[i] = field;
			i++;
		}

		if (forSingleRow) {
			return new SingleRowSheet(subset);
		}
		return new MultiRowsSheet(subset);
	}

	/**
	 * read, in our vocabulary, is ALWAYS primary key based read. Hence we
	 * expect (at most) one row of output per read. If values has more than one
	 * rows, we read for primary key in each row.
	 *
	 * @param inSheet
	 *            one or more rows that has value for the primary key.
	 * @param driver
	 * @param userId
	 *            we may have a common security strategy to restrict output rows
	 *            based on user. Not used as of now
	 * @return data sheet with data, or null if there is no data output.
	 *
	 */
	public DataSheet readMany(DataSheet inSheet, DbDriver driver, Value userId) {
		if (this.allPrimaryKeys == null) {
			throw new ApplicationError("Record " + this.name
					+ " is not defined with a primary key, and hence we can not do a read operation on this.");
		}
		int nbrRows = inSheet.length();
		if (nbrRows == 0) {
			return null;
		}

		DataSheet outSheet = this.createSheet(false, false);
		boolean singleRow = nbrRows == 1;
		if (singleRow) {
			Value[] values = this.getPrimaryKeyValues(inSheet, 0);
			if (values == null) {
				logger.info("Primary key value not available and hence no read operation.");
				return null;
			}
			int n = driver.extractFromSql(this.readSql, values, outSheet, singleRow);
			if (n == 0) {
				return null;
			}
			return outSheet;
		}

		Value[][] values = new Value[nbrRows][];
		for (int i = 0; i < nbrRows; i++) {
			Value[] vals = this.getPrimaryKeyValues(inSheet, i);
			if (vals == null) {
				logger.info("Primary key value not available and hence no read operation.");
				return null;
			}
			values[i] = vals;
		}
		driver.extractFromSql(this.readSql, values, outSheet);
		if (this.encryptedFields != null) {
			this.crypt(outSheet, true);
		}
		return outSheet;
	}

	private Value[] getPrimaryKeyValues(DataSheet inSheet, int idx) {
		Value[] values = new Value[this.allPrimaryKeys.length];
		for (int i = 0; i < this.allPrimaryKeys.length; i++) {
			Value value = inSheet.getColumnValue(this.allPrimaryKeys[i].name, idx);
			if (Value.isNull(value)) {
				return null;
			}
			values[i] = value;
		}
		return values;
	}

	private Value[] getWhereValues(FieldsCollection inFields, boolean includeTimeStamp) {
		Value[] values;
		int nbr = this.allPrimaryKeys.length;
		if (includeTimeStamp) {
			values = new Value[nbr + 1];
			Value stamp = inFields.getValue(this.modifiedStampField.name);
			if (Value.isNull(stamp)) {
				throw new ApplicationError("Field " + this.modifiedStampField.name
						+ " is timestamp, and value is required for an update operation to check for concurrency.");
			}
			values[nbr] = Value.newTimestampValue(stamp);
		} else {
			values = new Value[nbr];
		}
		for (int i = 0; i < this.allPrimaryKeys.length; i++) {
			Value value = inFields.getValue(this.allPrimaryKeys[i].name);
			if (Value.isNull(value)) {
				return null;
			}
			values[i] = value;
		}
		return values;
	}

	/**
	 * read, in our vocabulary, is ALWAYS primary key based read. Hence we
	 * expect (at most) one row of output per read.
	 *
	 * @param inData
	 *            one or more rows that has value for the primary key.
	 * @param driver
	 * @param userId
	 *            we may have a common security strategy to restrict output rows
	 *            based on user. Not used as of now
	 * @return Single row data sheet, or null if there is no data
	 */
	public DataSheet readOne(FieldsCollection inData, DbDriver driver, Value userId) {
		if (this.allPrimaryKeys == null) {
			throw new ApplicationError(
					"Record " + this.name + " is not defined with a primary key but a request is made for read.");
		}
		Value[] values = this.getWhereValues(inData, false);
		if (values == null) {

			logger.info("Value for primary key not present, and hence no read operation.");

			return null;
		}
		DataSheet outData = null;
		if (this.okToCache) {
			outData = this.getRowFromCache(inData);
			if (outData != null) {
				return outData;
			}
		}
		outData = this.createSheet(true, false);
		int nbr = driver.extractFromSql(this.readSql, values, outData, true);
		if (this.encryptedFields != null && nbr > 0) {
			this.crypt(outData, true);
		}
		if (this.okToCache) {
			this.cacheRow(inData, outData);
		}
		return outData;
	}

	/**
	 * checks if there is a row for this key. Row is not read.
	 *
	 * @param inData
	 * @param keyFieldName
	 * @param driver
	 * @param userId
	 * @return true if there is a row for this key, false otherwise. Row is not
	 *         read.
	 */
	public boolean rowExistsForKey(FieldsCollection inData, String keyFieldName, DbDriver driver, Value userId) {
		if (this.allPrimaryKeys == null) {

			logger.info("Record " + this.name
					+ " is not defined with a primary key, and hence we can not do a read operation on this.");

			this.noPrimaryKey();
			return false;
		}
		Value[] values;
		if (keyFieldName != null) {
			if (this.allPrimaryKeys.length > 1) {

				logger.info("There are more than one primary keys, and hence supplied name keyFieldName of "
						+ keyFieldName + " is ognored");

				values = this.getWhereValues(inData, false);
			} else {
				Value value = inData.getValue(keyFieldName);
				if (Value.isNull(value)) {

					logger.info("Primary key field " + keyFieldName + " has no value, and hence no read operation.");

					return false;
				}
				values = new Value[1];
				values[0] = value;
			}
		} else {
			values = this.getWhereValues(inData, false);
		}
		return driver.hasResult(this.readSql, values);
	}

	/**
	 * filter rows from underlying view/table as per filtering criterion
	 *
	 * @param inputRecord
	 *            record that has fields for filter criterion
	 * @param userId
	 *            we may have a common security strategy to restrict output rows
	 *            based on user. Not used as of now
	 * @param inData
	 *            as per filtering conventions
	 * @param driver
	 * @return data sheet, possible with retrieved rows
	 */
	public DataSheet filter(Record inputRecord, FieldsCollection inData, DbDriver driver, Value userId) {
		/*
		 * we have to create where clause with ? and corresponding values[]
		 */
		StringBuilder sql = new StringBuilder(this.filterSql);
		List<Value> filterValues = new ArrayList<Value>();
		boolean firstTime = true;
		for (Field field : inputRecord.fields) {
			String fieldName = field.name;
			Value value = inData.getValue(fieldName);
			if (Value.isNull(value) || value.toString().isEmpty()) {
				continue;
			}
			if (firstTime) {
				firstTime = false;
			} else {
				sql.append(" AND ");
			}

			FilterCondition condition = FilterCondition.Equal;
			Value otherValue = inData.getValue(fieldName + ServiceProtocol.COMPARATOR_SUFFIX);
			if (otherValue != null && otherValue.isUnknown() == false) {
				String text = otherValue.toString();
				/*
				 * it could be raw text like "~" or parsed value like
				 * "GreaterThan"
				 */
				condition = FilterCondition.valueOf(text);
				if (condition == null) {
					condition = FilterCondition.parse(text);
				}
				if (condition == null) {
					throw new ApplicationError(
							"Context hs an invalid filter condition of " + text + " for field " + fieldName);
				}
			}

			/** handle the special case of in-list */
			if (condition == FilterCondition.In) {
				Value[] values = Value.parse(value.toString().split(","), field.getValueType());
				/*
				 * we are supposed to have validated this at the input gate...
				 * but playing it safe
				 */
				if (values == null) {
					throw new ApplicationError(value + " is not a valid comma separated list for field " + field.name);
				}
				sql.append(field.externalName).append(" in (?");
				filterValues.add(values[0]);
				for (int i = 1; i < values.length; i++) {
					sql.append(",?");
					filterValues.add(values[i]);
				}
				sql.append(") ");
				continue;
			}

			if (condition == FilterCondition.Like) {
				value = Value.newTextValue(Record.PERCENT + DbDriver.escapeForLike(value.toString()) + Record.PERCENT);
			} else if (condition == FilterCondition.StartsWith) {
				value = Value.newTextValue(DbDriver.escapeForLike(value.toString()) + Record.PERCENT);
			}

			if (field.externalName == null) {
				logger.warn(
						"We reached a record field with no column name for {}. Lookslike some one did not trigger getReady()",
						fieldName);
				field.externalName = field.name;
			}

			sql.append(field.externalName).append(condition.getSql()).append("?");
			filterValues.add(value);

			if (condition == FilterCondition.Between) {
				otherValue = inData.getValue(fieldName + ServiceProtocol.TO_FIELD_SUFFIX);
				if (otherValue == null || otherValue.isUnknown()) {
					throw new ApplicationError("To value not supplied for field " + this.name + " for filtering");
				}
				sql.append(" AND ?");
				filterValues.add(otherValue);
			}
		}
		Value[] values;
		if (firstTime) {
			/*
			 * no conditions..
			 */
			if (this.okToSelectAll == false) {
				throw new ApplicationError("Record " + this.name
						+ " is likely to contain large number of records, and hence we do not allow select-all operation");
			}
			sql.append(" 1 = 1 ");
			values = new Value[0];
		} else {
			values = filterValues.toArray(new Value[0]);
		}
		/*
		 * is there sort order?
		 */
		Value sorts = inData.getValue(ServiceProtocol.SORT_COLUMN_NAME);
		if (sorts != null) {
			sql.append(" ORDER BY ").append(sorts.toString());
		}

		DataSheet result = this.createSheet(false, false);
		driver.extractFromSql(sql.toString(), values, result, false);
		if (this.encryptedFields != null) {
			this.crypt(result, true);
		}
		return result;
	}

	/**
	 * add, modify and delete are the three operations we can do for a record.
	 * "save" is a special convenient command. If key is specified, it is
	 * assumed to be modify, else add. Save
	 *
	 * @param row
	 *            data to be saved.
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 *            if true, we assume that some constraints are set at db level,
	 *            and sql error is treated as if affected rows is zero
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public SaveActionType saveOne(FieldsCollection row, DbDriver driver, Value userId,
			boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}

		if (this.allPrimaryKeys == null) {
			this.noPrimaryKey();
		}
		Field pkey = this.allPrimaryKeys[0];
		Value[] values = new Value[this.fields.length];
		/*
		 * modified user field, even if sent by client, must be over-ridden
		 */
		if (this.modifiedUserField != null) {
			row.setValue(this.modifiedUserField.name, userId);
		}
		/*
		 * is the action explicitly specified
		 */
		SaveActionType saveAction = SaveActionType.SAVE;
		Value action = row.getValue(ServiceProtocol.TABLE_ACTION_FIELD_NAME);
		if (action != null) {
			/*
			 * since this field is extracted by us earlier, we DO KNOW that it
			 * is valid
			 */
			saveAction = SaveActionType.parse(action.toString());
		}
		if (saveAction == SaveActionType.SAVE) {
			/*
			 * is the key supplied?
			 */
			Value keyValue = row.getValue(pkey.name);
			if (this.keyToBeGenerated) {
				if (Value.isNull(keyValue)) {
					saveAction = SaveActionType.ADD;
				} else {
					saveAction = SaveActionType.MODIFY;
				}
			} else {
				if (this.rowExistsForKey(row, null, driver, userId)) {
					saveAction = SaveActionType.MODIFY;
				} else {
					saveAction = SaveActionType.ADD;
				}
			}
		}
		if (saveAction == SaveActionType.ADD) {
			if (this.createdUserField != null) {
				row.setValue(this.createdUserField.name, userId);
			}
			values = this.getInsertValues(row, userId);
			if (this.keyToBeGenerated) {
				long[] generatedKeys = new long[1];
				String[] generatedColumns = { pkey.externalName };
				driver.insertAndGetKeys(this.insertSql, values, generatedKeys, generatedColumns,
						treatSqlErrorAsNoResult);
				row.setValue(pkey.name, Value.newIntegerValue(generatedKeys[0]));
			} else {
				driver.executeSql(this.insertSql, values, treatSqlErrorAsNoResult);
			}
		} else if (saveAction == SaveActionType.DELETE) {
			values = this.getWhereValues(row, this.useTimestampForConcurrency);
			driver.executeSql(this.deleteSql, values, treatSqlErrorAsNoResult);
		} else {
			values = this.getUpdateValues(row, userId);
			if (driver.executeSql(this.updateSql, values, treatSqlErrorAsNoResult) == 0) {
				throw new ApplicationError(
						"Data was changed by some one else while you were editing it. Please cancel this operation and redo it with latest data.");
			}
		}
		return saveAction;
	}

	/**
	 * add, modify and delete are the three operations we can do for a record.
	 * "save" is a special convenient command. If key is specified, it is
	 * assumed to be modify, else add. Save
	 *
	 * @param inSheet
	 *            data to be saved.
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public SaveActionType[] saveMany(DataSheet inSheet, DbDriver driver, Value userId,
			boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}
		SaveActionType[] result = new SaveActionType[inSheet.length()];
		int rowIdx = 0;
		for (FieldsCollection row : inSheet) {
			result[rowIdx] = this.saveOne(row, driver, userId, treatSqlErrorAsNoResult);
			rowIdx++;
		}
		return result;
	}

	/**
	 * parent record got saved. we are to save rows for this record
	 *
	 * @param inSheet
	 *            data for this record
	 * @param parentRow
	 *            data for parent record that is already saved
	 * @param actions
	 *            that are already done using parent sheet
	 * @param driver
	 * @param userId
	 * @return number of rows affected
	 */
	public int saveWithParent(DataSheet inSheet, FieldsCollection parentRow, SaveActionType[] actions, DbDriver driver,
			Value userId) {
		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allParentKeys == null) {
			this.noParent();
		}
		/*
		 * for security/safety, we copy parent key into data
		 */
		this.copyParentKeys(parentRow, inSheet);
		for (FieldsCollection row : inSheet) {
			this.saveOne(row, driver, userId, false);
		}
		return inSheet.length();
	}

	/**
	 * @param inSheet
	 * @param userId
	 * @param rowIdx
	 * @return
	 */
	private Value[] getInsertValues(FieldsCollection row, Value userId) {
		/*
		 * we may not fill all fields, but let us handle that exception later
		 */
		Value[] values = new Value[this.nbrInsertFields];
		int valueIdx = 0;
		for (Field field : this.fields) {
			if (field.canInsert() == false) {
				continue;
			}
			if (field.fieldType == FieldType.CREATED_BY_USER || field.fieldType == FieldType.MODIFIED_BY_USER) {
				values[valueIdx] = userId;
			} else {
				Value value = field.getValue(row);
				if (Value.isNull(value)) {
					if (field.isNullable) {
						value = Value.newUnknownValue(field.getValueType());
					} else {
						throw new ApplicationError("Column " + field.externalName + " in table " + this.tableName
								+ " is designed to be non-null, but a row is being inserted with a null value in it.");
					}
				}
				if (field.isEncrypted) {
					value = this.crypt(value, false);
				}
				values[valueIdx] = value;
			}
			valueIdx++;
		}
		return values;
	}

	/**
	 * insert row/s
	 *
	 * @param inSheet
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int insert(DataSheet inSheet, DbDriver driver, Value userId, boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}
		int nbrRows = inSheet.length();

		/*
		 * simple case first...
		 */
		if (nbrRows == 1) {
			return this.insert((FieldsCollection) inSheet, driver, userId, treatSqlErrorAsNoResult);
		}
		Value[][] allValues = new Value[nbrRows][];
		/*
		 * we mostly expect one row, but we do not want to write separate
		 * code...
		 */
		int rowIdx = 0;
		for (FieldsCollection row : inSheet) {
			allValues[rowIdx] = this.getInsertValues(row, userId);
			rowIdx++;
		}
		if (this.keyToBeGenerated == false) {
			return this.executeWorker(driver, this.insertSql, allValues, treatSqlErrorAsNoResult);
		}
		long[] generatedKeys = new long[nbrRows];
		int result = this.insertWorker(driver, this.insertSql, allValues, generatedKeys, treatSqlErrorAsNoResult);
		if (result > 0 && generatedKeys[0] != 0) {
			this.addKeyColumn(inSheet, generatedKeys);
		}

		return result;
	}

	/**
	 * insert row/s
	 *
	 * @param inData
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int insert(FieldsCollection inData, DbDriver driver, Value userId, boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}
		Value[][] allValues = new Value[1][];
		allValues[0] = this.getInsertValues(inData, userId);

		if (this.keyToBeGenerated == false) {
			return this.executeWorker(driver, this.insertSql, allValues, treatSqlErrorAsNoResult);
		}
		/*
		 * try to get generated keys and set it/them
		 */
		long[] generatedKeys = new long[1];
		int result = this.insertWorker(driver, this.insertSql, allValues, generatedKeys, treatSqlErrorAsNoResult);
		if (result > 0) {
			/*
			 * generated key feature may not be available with some rdb vendor
			 */
			long key = generatedKeys[0];
			if (key > 0) {
				inData.setValue(this.allPrimaryKeys[0].name, Value.newIntegerValue(key));
			}
		}

		return result;
	}

	/**
	 * insert row/s
	 *
	 * @param inSheet
	 *            data for this record to be inserted inserted after its parent
	 *            got inserted
	 * @param parentRow
	 *            fields/row that has the parent key
	 * @param driver
	 * @param userId
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int insertWithParent(DataSheet inSheet, FieldsCollection parentRow, DbDriver driver, Value userId) {
		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allParentKeys == null) {
			this.noParent();
		}

		/*
		 * for security/safety, we copy parent key into data
		 */
		this.copyParentKeys(parentRow, inSheet);

		int nbrRows = inSheet.length();
		Value[][] allValues = new Value[nbrRows][];
		int rowIdx = 0;

		for (FieldsCollection row : inSheet) {
			allValues[rowIdx] = this.getInsertValues(row, userId);
			rowIdx++;
		}
		if (this.keyToBeGenerated == false) {
			return this.executeWorker(driver, this.insertSql, allValues, false);
		}
		/*
		 * generated key is t be retrieved
		 */
		long[] keys = new long[nbrRows];
		int result = this.insertWorker(driver, this.insertSql, allValues, keys, false);
		if (keys[0] != 0) {
			this.addKeyColumn(inSheet, keys);
		}
		return result;
	}

	/**
	 * add a column to the data sheet and copy primary key values into that
	 *
	 * @param inSheet
	 * @param keys
	 */
	private void addKeyColumn(DataSheet inSheet, long[] keys) {
		int nbrKeys = keys.length;
		Value[] values = new Value[nbrKeys];
		int i = 0;
		for (long key : keys) {
			values[i++] = Value.newIntegerValue(key);
		}
		inSheet.addColumn(this.allPrimaryKeys[0].name, ValueType.INTEGER, values);
	}

	/**
	 * copy parent key to child sheet
	 *
	 * @param inData
	 *            row/fields that has the parent key
	 * @param sheet
	 *            to which we have to copy the key values
	 */
	private void copyParentKeys(FieldsCollection inData, DataSheet sheet) {
		for (Field field : this.allParentKeys) {
			String fieldName = field.name;
			String parentKeyName = field.referredField;
			Value parentKey = inData.getValue(parentKeyName);
			if (Value.isNull(parentKey)) {

				logger.info("No value found for parent key field " + parentKeyName
						+ " and hence no column is going to be added to child table");

				return;
			}
			sheet.addColumn(fieldName, parentKey);
		}
	}

	/**
	 * get parent key values
	 *
	 * @param inData
	 *            row/fields that has the parent key
	 * @param sheet
	 *            to which we have to copy the key values
	 */
	private Value[] getParentValues(FieldsCollection inData) {
		Value[] values = new Value[this.allParentKeys.length];
		for (int i = 0; i < this.allParentKeys.length; i++) {
			Field field = this.allParentKeys[i];
			Value value = inData.getValue(field.referredField);
			if (Value.isNull(value)) {

				logger.info("No value found for parent key field " + field.referredField
						+ " and hence no column is going to be added to child table");

				return null;
			}
			values[i] = value;
		}
		return values;
	}

	/**
	 * update all fields, except of course the ones that are not to be.
	 *
	 * @param inSheet
	 *            data to be saved.
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int update(DataSheet inSheet, DbDriver driver, Value userId, boolean treatSqlErrorAsNoResult) {

		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allPrimaryKeys == null) {
			this.noPrimaryKey();
		}
		int nbrRows = inSheet.length();
		Value[][] allValues = new Value[nbrRows][];
		if (nbrRows == 1) {
			allValues[0] = this.getUpdateValues(inSheet, userId);
		} else {
			int i = 0;
			for (FieldsCollection row : inSheet) {
				allValues[i++] = this.getUpdateValues(row, userId);
			}
		}
		int result = this.executeWorker(driver, this.updateSql, allValues, treatSqlErrorAsNoResult);

		if (result > 0 && this.recordsToBeNotifiedOnChange != null) {
			for (FieldsCollection row : inSheet) {
				this.invalidateCache(row);
			}
		}
		return result;
	}

	/**
	 * update all fields, except of course the ones that are not to be.
	 *
	 * @param inputData
	 *            data to be saved.
	 * @param driver
	 * @param userId
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int update(FieldsCollection inputData, DbDriver driver, Value userId, boolean treatSqlErrorAsNoResult) {

		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allPrimaryKeys == null) {
			this.noPrimaryKey();
		}
		Value[][] allValues = new Value[1][];

		allValues[0] = this.getUpdateValues(inputData, userId);
		int result = this.executeWorker(driver, this.updateSql, allValues, treatSqlErrorAsNoResult);
		if (result > 0 && this.recordsToBeNotifiedOnChange != null) {
			this.invalidateCache(inputData);
		}
		return result;
	}

	/**
	 * update all fields, except of course the ones that are not to be.
	 *
	 * @param inSheet
	 *            data to be saved.
	 * @param driver
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int delete(DataSheet inSheet, DbDriver driver, boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allPrimaryKeys == null) {
			this.noPrimaryKey();
		}
		int nbrRows = inSheet.length();
		/*
		 * we mostly expect one row, but we do not want to write separate
		 * code...
		 */
		Value[][] allValues = new Value[nbrRows][];
		nbrRows = 0;
		for (FieldsCollection row : inSheet) {
			allValues[nbrRows++] = this.getWhereValues(row, this.useTimestampForConcurrency);
		}
		int result = this.executeWorker(driver, this.deleteSql, allValues, treatSqlErrorAsNoResult);

		if (result > 0 && this.recordsToBeNotifiedOnChange != null) {
			for (FieldsCollection row : inSheet) {
				this.invalidateCache(row);
			}
		}
		return result;
	}

	/**
	 * update all fields, except of course the ones that are not to be.
	 *
	 * @param inData
	 *            data to be saved.
	 * @param driver
	 * @param treatSqlErrorAsNoResult
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int delete(FieldsCollection inData, DbDriver driver, boolean treatSqlErrorAsNoResult) {
		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allPrimaryKeys == null) {
			this.noPrimaryKey();
		}
		Value[][] allValues = new Value[1][];
		allValues[0] = this.getWhereValues(inData, this.useTimestampForConcurrency);
		int result = this.executeWorker(driver, this.deleteSql, allValues, treatSqlErrorAsNoResult);

		if (result > 0 && this.recordsToBeNotifiedOnChange != null) {
			this.invalidateCache(inData);
		}
		return result;
	}

	private void notWritable() {
		throw new ApplicationError("Record " + this.name
				+ " is not designed to be writable. Add/Update/Delete operations are not possible.");
	}

	private void noParent() {
		throw new ApplicationError(
				"Record " + this.name + " does not have a parent key field. Operation with parent is not possible.");
	}

	private void noPrimaryKey() {
		throw new ApplicationError("Update/Delete operations are not possible for Record " + this.name
				+ " as it does not define a primary key.");
	}

	/**
	 * delete child rows for this record when its parent is deleted.
	 *
	 * @param parentRow
	 *            from where we pick up parent key.
	 * @param driver
	 * @param userId
	 * @return number of rows saved. -1 in case of batch, and the driver is
	 *         unable to count the saved rows
	 */
	public int deleteWithParent(FieldsCollection parentRow, DbDriver driver, Value userId) {
		if (this.readOnly) {
			this.notWritable();
		}
		if (this.allParentKeys == null) {
			this.noParent();
		}
		Value[] values = this.getParentValues(parentRow);
		if (values == null) {

			logger.info("Delete with parent has nothing to delete as parent key is null");

			return 0;
		}
		StringBuilder sql = new StringBuilder("DELETE FROM ");
		sql.append(this.tableName).append(" WHERE ").append(this.getParentWhereClause());
		if (this.allParentKeys.length > 1) {
			for (int i = 1; i < this.allParentKeys.length; i++) {
				sql.append(" AND " + this.allParentKeys[i].externalName + EQUAL_PARAM);
			}
		}
		return driver.executeSql(sql.toString(), values, false);
	}

	private int executeWorker(DbDriver driver, String sql, Value[][] values, boolean treatSqlErrorAsNoResult) {
		if (values.length == 1) {
			return driver.executeSql(sql, values[0], treatSqlErrorAsNoResult);
		}
		int[] counts = driver.executeBatch(sql, values, treatSqlErrorAsNoResult);
		int result = 0;
		for (int n : counts) {
			if (n < 0) {
				return -1;
			}
			result += n;
		}
		return result;
	}

	private int insertWorker(DbDriver driver, String sql, Value[][] values, long[] generatedKeys,
			boolean treatSqlErrorAsNoResult) {
		String[] keyNames = { this.allPrimaryKeys[0].externalName };
		if (values.length == 1) {
			return driver.insertAndGetKeys(sql, values[0], generatedKeys, keyNames, treatSqlErrorAsNoResult);
		}
		if (generatedKeys != null) {

			logger.info(
					"Generated key retrieval is NOT supported for batch. Keys for child table are to be retrieved automatically");
		}
		int[] counts = driver.executeBatch(sql, values, treatSqlErrorAsNoResult);
		int result = 0;
		for (int n : counts) {
			if (n < 0) {
				return -1;
			}
			result += n;
		}
		return result;
	}

	/**
	 * read rows from this record for a given parent record
	 *
	 * @param parentData
	 *            rows for parent
	 * @param driver
	 * @param sheetName
	 * @param cascadeFilter
	 * @param ctx
	 */
	public void filterForParents(DataSheet parentData, DbDriver driver, String sheetName, boolean cascadeFilter,
			ServiceContext ctx) {
		DataSheet result = this.createSheet(false, false);
		int n = parentData.length();
		if (n == 0) {
			return;
		}
		if (this.allParentKeys.length > 1) {
			this.filterForMultiParentKeys(parentData, driver, result);
		} else {
			this.filterForSingleParentKey(parentData, driver, result);
		}
		String sn = sheetName;
		if (sn == null) {
			sn = this.getDefaultSheetName();
		}
		ctx.putDataSheet(sn, result);

		if (result.length() > 0 && cascadeFilter) {
			this.filterChildRecords(result, driver, ctx);
		}
	}

	/**
	 * @param parentData
	 * @param driver
	 * @param result
	 */
	private void filterForSingleParentKey(DataSheet parentData, DbDriver driver, DataSheet result) {
		String keyName = this.allParentKeys[0].referredField;
		int n = parentData.length();
		Value[] values = parentData.getColumnValues(keyName);
		StringBuilder sbf = new StringBuilder(this.filterSql);
		sbf.append(this.allParentKeys[0].externalName);
		/*
		 * for single key we use where key = ?
		 *
		 * for multiple, we use where key in (?,?,....)
		 */
		if (n == 1) {
			sbf.append(EQUAL_PARAM);
		} else {
			sbf.append(" IN (?");
			for (int i = 1; i < n; i++) {
				sbf.append(",?");
			}
			sbf.append(')');
		}
		driver.extractFromSql(sbf.toString(), values, result, false);
		if (this.encryptedFields != null) {
			this.crypt(result, true);
		}
	}

	/**
	 * read rows from this record for a given parent record
	 *
	 * @param parentData
	 *            rows for parent
	 * @param driver
	 * @param name
	 * @param cascadeFilter
	 * @param ctx
	 */
	private void filterForMultiParentKeys(DataSheet parentData, DbDriver driver, DataSheet outSheet) {
		String sql = this.filterSql + this.getParentWhereClause();
		int nbrRows = parentData.length();
		Value[][] allValues = new Value[nbrRows][];
		int idx = 0;
		for (FieldsCollection prentRow : parentData) {
			allValues[idx++] = this.getParentValues(prentRow);
		}
		driver.extractFromSql(sql, allValues, outSheet);
		if (this.encryptedFields != null) {
			this.crypt(outSheet, true);
		}
	}

	/**
	 * if this record has child records, filter them based on this parent sheet
	 *
	 * @param parentSheet
	 *            sheet that has rows for this record
	 * @param driver
	 * @param ctx
	 */
	public void filterChildRecords(DataSheet parentSheet, DbDriver driver, ServiceContext ctx) {
		if (this.childrenToBeRead == null) {
			return;
		}
		for (String childName : this.childrenToBeRead) {
			Record cr = ComponentManager.getRecord(childName);
			cr.filterForParents(parentSheet, driver, cr.getDefaultSheetName(), true, ctx);
		}
	}

	/**
	 * read rows from this record for a given parent record
	 *
	 * @param parentData
	 * @param driver
	 * @return sheet that contains rows from this record for the parent rows
	 */
	public DataSheet filterForAParent(FieldsCollection parentData, DbDriver driver) {
		DataSheet result = this.createSheet(false, false);
		Value[] values = this.getParentValues(parentData);
		String sql = this.filterSql + this.getParentWhereClause();
		driver.extractFromSql(sql, values, result, false);
		if (this.encryptedFields != null) {
			this.crypt(result, true);
		}
		return result;
	}

	private String getParentWhereClause() {
		StringBuilder sbf = new StringBuilder();
		sbf.append(this.allParentKeys[0].externalName).append(EQUAL_PARAM);
		for (int i = 1; i < this.allParentKeys.length; i++) {
			sbf.append(" AND ").append(this.allParentKeys[i].externalName).append(EQUAL_PARAM);
		}
		return sbf.toString();
	}

	/**
	 * en/de crypt a value
	 *
	 * @param value
	 * @param toDecrypt
	 *            true means decrypt, else encrypt
	 * @return
	 */
	private Value crypt(Value value, boolean toDecrypt) {
		if (Value.isNull(value)) {
			return value;
		}
		String txt = value.toString();
		if (toDecrypt) {
			txt = TextUtil.decrypt(txt);
		} else {
			txt = TextUtil.encrypt(txt);
		}
		return Value.newTextValue(txt);
	}

	/**
	 * encrypt/decrypt columns in this data sheet
	 *
	 * @param sheet
	 * @param toDecrypt
	 *            true means decrypt, else encrypt
	 */
	private void crypt(DataSheet sheet, boolean toDecrypt) {
		int nbrRows = sheet.length();
		if (nbrRows == 0) {

			logger.info("Data sheet has no data and hance no encryption.");

			return;
		}
		for (Field field : this.encryptedFields) {
			int colIdx = sheet.getColIdx(field.name);
			if (colIdx == -1) {

				logger.info("Data sheet has no column named " + field.name + " hance no encryption.");

				continue;
			}
			for (int rowIdx = 0; rowIdx < nbrRows; rowIdx++) {
				Value[] row = sheet.getRow(rowIdx);
				row[colIdx] = this.crypt(row[colIdx], toDecrypt);
			}
		}

		logger.info(nbrRows + " rows and " + this.encryptedFields.length + " columns " + (toDecrypt ? "un-" : "")
				+ "obsfuscated");
	}

	/*
	 * we have a possible issue with referred records. If A refers to B and B
	 * refers to A, we have an error on hand. How do we track this? as of now,
	 * we will track this during getReady() invocation. getReady() will ask for
	 * a referred record. That record will execute getReady() before returning.
	 * It may ask for another record, so an and so forth.
	 *
	 * There are two ways to solve this problem.
	 *
	 * One way is to differentiate between normal-request and reference-request
	 * for a record. Pass history during reference request so that we can detect
	 * circular reference. Issue with this is that getRequest() is a generic
	 * method and hence we can not customize it.
	 *
	 * Other approach is to use thread-local that is initiated by getReady().
	 *
	 * our algorithm is :
	 *
	 * 1. we initiate refHistory before getReady() and current record to
	 * pendingOnes.
	 *
	 * 2. A referred field may invoke parent.getRefrecord() Referred record is
	 * requested from ComponentManager.getRecord();
	 *
	 * 3. that call will trigger getReady() on the referred record. This chain
	 * will go-on..
	 *
	 * 4. before adding to pending list we check if it already exists. That
	 * would be a circular reference.
	 *
	 * 5. Once we complete getReady(), we remove this record from pendingOnes.
	 * And if there no more pending ones, we remove it. and that completes the
	 * check.
	 */
	/**
	 * tracks recursive reference calls between records and referred records for
	 * referred fields
	 */
	static ThreadLocal<RefHistory> referenceHistory = new ThreadLocal<Record.RefHistory>();

	class RefHistory {
		/** recursive reference history of record names */
		List<String> pendingOnes = new ArrayList<String>();
		/** records that have completed loading as part of this process */
		Map<String, Record> finishedOnes = new HashMap<String, Record>();
	}

	/**
	 * called from field when it refers to a field in another record
	 *
	 * @param recordName
	 * @return
	 */
	Record getRefRecord(String recordName) {
		RefHistory history = referenceHistory.get();
		if (history == null) {
			throw new ApplicationError("Record.java has an issue with getReady() logic. history is null");
		}
		/*
		 * do we have it in our cache?
		 */
		Record record = history.finishedOnes.get(recordName);
		if (record != null) {
			return record;
		}
		/*
		 * is this record already in the pending list?
		 */
		if (history.pendingOnes.contains(recordName)) {
			/*
			 * we have a circular reference issue.
			 */
			StringBuilder sbf = new StringBuilder();
			if (recordName.equals(this.getQualifiedName())) {
				sbf.append("Record ").append(recordName).append(
						" has at least one field that refers to another field in this record itself. Sorry, you can't do that.");
			} else {
				sbf.append(
						"There is a circular reference of records amongst the following records. Please review and fix.\n{\n");
				int nbr = history.pendingOnes.size();
				for (int i = 0; i < nbr; i++) {

					sbf.append(i).append(": ").append(history.pendingOnes.get(i)).append('\n');
				}
				sbf.append(nbr).append(": ").append(recordName).append('\n');
				sbf.append('}');
			}
			throw new ApplicationError(sbf.toString());
		}
		return ComponentManager.getRecord(recordName);
	}

	/**
	 * called before starting getReady()
	 *
	 * @return true if we initiated the trail..
	 */
	private boolean recordGettingReady() {
		String recName = this.getQualifiedName();
		RefHistory history = referenceHistory.get();
		if (history == null) {
			history = new RefHistory();
			history.pendingOnes.add(recName);
			referenceHistory.set(history);
			return true;
		}
		if (history.pendingOnes.contains(recName) == false) {
			history.pendingOnes.add(recName);
			return false;
		}
		/*
		 * we have a circular reference issue.
		 */

		StringBuilder sbf = new StringBuilder();
		if (history.pendingOnes.size() == 1) {
			sbf.append("Record ").append(recName).append(
					" has at least one field that refers to another field in this record itself. Sorry, you can't do that.");
		} else {
			sbf.append(
					"There is a circular reference of records amongst the following records. Please review and fix.\n{\n");
			int nbr = history.pendingOnes.size();
			for (int i = 0; i < nbr; i++) {

				sbf.append(i).append(". ").append(history.pendingOnes.get(i)).append('\n');
			}
			sbf.append(nbr).append(". ").append(recName).append('\n');
			sbf.append('}');
		}

		logger.info(sbf.toString());

		return false;
		// throw new ApplicationError(sbf.toString());
	}

	/** called at the end of getReady(); */
	private void recordGotReady(boolean originator) {
		String recName = this.getQualifiedName();
		RefHistory history = referenceHistory.get();
		if (history == null) {

			logger.info("There is an issue with the way Record " + recName
					+ "  is trying to detect circular reference. History has disappeared.");

			return;
		}
		if (originator == false) {
			history.pendingOnes.remove(recName);
			history.finishedOnes.put(recName, this);
			return;
		}
		if (history.pendingOnes.size() > 1) {
			StringBuilder sbf = new StringBuilder();
			for (String s : history.pendingOnes) {
				sbf.append(s).append(' ');
			}

			logger.info("There is an issue with the way Record " + recName
					+ "  is trying to detect circular reference. pending list remained as " + sbf.toString());
		}
		referenceHistory.remove();
	}

	@Override
	public void getReady() {
		if (TABLE_ACTION_FIELD == null) {
			TABLE_ACTION_FIELD = Field.getDefaultField(TABLE_ACTION_FIELD_NAME, ValueType.TEXT);
		}
		if (this.fields == null) {
			throw new ApplicationError("Record " + this.getQualifiedName() + " has no fields.");
		}
		if (this.tableName == null) {
			this.tableName = this.name;
		}
		if (this.defaultSheetName == null) {
			this.defaultSheetName = this.name;
		}
		if (this.recordType != RecordUsageType.STORAGE) {
			this.readOnly = true;
		}

		if (this.keyToBeGenerated) {
			if (DbDriver.generatorNameRequired()) {
				if (this.sequenceName == null) {
					this.sequence = this.tableName + DEFAULT_SEQ_SUFFIX;

					logger.info("sequence not specified for table " + this.tableName + ". default sequence name  "
							+ this.sequence + " is assumed.");

				} else {
					this.sequence = this.sequenceName + ".NEXTVAL";
				}
			}
		}
		/*
		 * we track referred records. push to stack
		 */
		boolean originator = this.recordGettingReady();
		Record refRecord = null;
		if (this.defaultRefRecord != null) {
			refRecord = this.getRefRecord(this.defaultRefRecord);
		}

		this.fieldNames = new String[this.fields.length];
		int nbrPrimaries = 0;
		int nbrParents = 0;
		int nbrEncrypted = 0;

		for (int i = 0; i < this.fields.length; i++) {
			Field field = this.fields[i];
			if (this.forFixedWidthRow) {
				this.minRecordLength += field.fieldWidth;
			}
			field.getReady(this, refRecord, this.recordType == RecordUsageType.VIEW);
			String fName = field.name;
			this.fieldNames[i] = fName;
			this.indexedFields.put(fName, field);
			if (!this.hasInterFieldValidations && field.hasInterFieldValidations()) {
				this.hasInterFieldValidations = true;
			}
			if (field.isEncrypted) {
				nbrEncrypted++;
			}
			FieldType ft = field.getFieldType();
			if (FieldType.isPrimaryKey(ft)) {
				nbrPrimaries++;
			}
			if (FieldType.isParentKey(ft)) {
				nbrParents++;
			}
			if (ft == FieldType.MODIFIED_TIME_STAMP) {
				this.checkDuplicateError(this.modifiedStampField);
				this.modifiedStampField = field;
			} else if (ft == FieldType.CREATED_BY_USER) {
				this.checkDuplicateError(this.createdUserField);
				this.createdUserField = field;
			} else if (ft == FieldType.MODIFIED_BY_USER) {
				this.checkDuplicateError(this.modifiedUserField);
				this.modifiedUserField = field;
			} else if (ft == FieldType.RECORD || ft == FieldType.RECORD_ARRAY || ft == FieldType.VALUE_ARRAY) {
				this.isComplexStruct = true;
			}
		}
		/*
		 * last field is allowed to be flexible in a fixed width record
		 */
		if (this.forFixedWidthRow) {
			this.minRecordLength -= this.fields[this.fields.length - 1].fieldWidth;
		}
		/*
		 * because of possible composite keys, we save keys in arrays
		 */
		if (nbrPrimaries > 0 || nbrParents > 0 || nbrEncrypted > 0) {
			this.cacheSpecialFields(nbrPrimaries, nbrParents, nbrEncrypted);
		}
		/*
		 * are we ok for concurrency check?
		 */
		if (this.useTimestampForConcurrency) {
			if (this.modifiedStampField == null) {
				throw new ApplicationError("Record " + this.name
						+ " has set useTimestampForConcurrency=true, but not has marked any field as modifiedAt.");
			}
			if (this.modifiedStampField.getValueType() != ValueType.TIMESTAMP) {
				throw new ApplicationError("Record " + this.name + " uses " + this.modifiedStampField.name
						+ " as modiedAt field, but has defined it as a " + this.modifiedStampField.getValueType()
						+ ". It should be defined as a TIMESTAMP for it to be used for concurrency check.");
			}
		}
		if (this.defaultSheetName == null) {
			this.defaultSheetName = this.name;
		}

		if (this.allPrimaryKeys != null) {
			this.setPrimaryWhere();
		}
		/*
		 * get ready with sqls for reading
		 */
		this.createReadSqls();

		/*
		 * is this record writable?
		 */
		if (this.readOnly == false) {
			this.createWriteSqls();
		}

		/*
		 * we have successfully loaded. remove this record from stack.
		 */
		this.recordGotReady(originator);
	}

	/**
	 * @param nbrPrimaries
	 */
	private void cacheSpecialFields(int nbrPrimaries, int nbrParents, int nbrEncrypted) {
		if (nbrPrimaries > 0) {
			this.allPrimaryKeys = new Field[nbrPrimaries];
		}
		if (nbrParents > 0) {
			this.allParentKeys = new Field[nbrParents];
		}
		if (nbrEncrypted > 0) {
			this.encryptedFields = new Field[nbrEncrypted];
		}
		int primaryIdx = 0;
		int parentIdx = 0;
		int encrIdx = 0;
		for (Field field : this.fields) {
			if (FieldType.isPrimaryKey(field.fieldType)) {
				this.allPrimaryKeys[primaryIdx] = field;
				primaryIdx++;
			}
			if (FieldType.isParentKey(field.fieldType)) {
				this.allParentKeys[parentIdx] = field;
				parentIdx++;
			}
			if (field.isEncrypted) {
				this.encryptedFields[encrIdx] = field;
				encrIdx++;
			}
		}
	}

	private void checkDuplicateError(Field savedField) {
		if (savedField == null) {
			return;
		}

		throw new ApplicationError("Record " + this.getQualifiedName() + " defines more than one field with field type "
				+ savedField.fieldType.name() + ". This feature is not supported");
	}

	/** Create read and filter sqls */
	private void createReadSqls() {

		/*
		 * start with read sqls
		 */
		StringBuilder select = new StringBuilder("SELECT ");

		boolean isFirstField = true;
		for (Field field : this.fields) {
			if (isFirstField) {
				isFirstField = false;
			} else {
				select.append(Record.COMMA);
			}
			select.append(field.externalName).append(" \"").append(field.name).append('"');
		}

		select.append(" FROM ").append(this.tableName);

		/*
		 * filter sql stops at where. Actual where clauses will be added at run
		 * time
		 */
		String selectText = select.toString();
		this.filterSql = selectText + " WHERE ";

		/*
		 * read is applicable if there is primary key
		 */
		if (this.allPrimaryKeys != null) {
			/*
			 * where clause is common across different sqls..
			 */
			this.readSql = selectText + this.primaryWhereClause;
		}

		if (this.listFieldName != null) {
			this.setListSql();
		}
		if (this.suggestionKeyName != null) {
			this.setSuggestSql();
		}
	}

	/**
	 * set sql strings. We are setting four fields at the end. For clarity, you
	 * should trace one string at a time and understand what we are trying to
	 * do. Otherwise it looks confusing
	 */
	private void createWriteSqls() {
		String timeStamp = DbDriver.getTimeStamp();
		/*
		 * we have two buffers for insert as fields are to be inserted at two
		 * parts
		 */
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(this.tableName).append('(');
		StringBuilder vals = new StringBuilder(") Values(");

		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(this.tableName).append(" SET ");

		boolean firstInsertField = true;
		boolean firstUpdatableField = true;
		for (Field field : this.fields) {
			/*
			 * some fields are not updatable
			 */
			if (field.canUpdate() || field.fieldType == FieldType.MODIFIED_TIME_STAMP) {
				if (firstUpdatableField) {
					firstUpdatableField = false;
				} else {
					update.append(COMMA);
				}
				update.append(field.externalName).append(Record.EQUAL);
				if (field.fieldType == FieldType.MODIFIED_TIME_STAMP) {
					update.append(timeStamp);
				} else {
					update.append(Record.PARAM);
					// this.nbrUpdateFields++;
				}
			}
			FieldType fieldType = field.fieldType;
			/*
			 * if primary key is managed by rdbms, we do not bother about it?
			 */
			if (FieldType.isPrimaryKey(fieldType) && this.keyToBeGenerated && this.sequence == null) {
				continue;
			}
			if (firstInsertField) {
				firstInsertField = false;
			} else {
				insert.append(Record.COMMA);
				vals.append(Record.COMMA);
			}
			insert.append(field.externalName);
			/*
			 * value is hard coded for time stamps
			 */
			if (field.fieldType == FieldType.MODIFIED_TIME_STAMP || field.fieldType == FieldType.CREATED_TIME_STAMP) {
				vals.append(timeStamp);
			} else if (FieldType.isPrimaryKey(fieldType) && this.keyToBeGenerated) {
				vals.append(this.sequence);
			} else {
				vals.append(Record.PARAM);
				this.nbrInsertFields++;
			}
		}
		/*
		 * set insert sql
		 */
		insert.append(vals.append(')'));
		this.insertSql = insert.toString();

		/*
		 * where clause of delete and update are same, but they are valid only
		 * if we have a primary key
		 */
		if (this.allPrimaryKeys != null) {
			if (this.useTimestampForConcurrency) {
				String clause = " AND " + this.modifiedStampField.externalName + "=?";
				this.updateSql = update.append(this.primaryWhereClause).append(clause).toString();
				this.deleteSql = "DELETE FROM " + this.tableName + this.primaryWhereClause + clause;

			} else {
				this.updateSql = update.append(this.primaryWhereClause).toString();
				this.deleteSql = "DELETE FROM " + this.tableName + this.primaryWhereClause;
			}
		}
	}

	/**
	 * set sql strings. We are setting four fields at the end. For clarity, you
	 * should trace one string at a time and understand what we are trying to
	 * do. Otherwise it looks confusing
	 *
	 * @param row
	 * @param userId
	 * @return
	 */
	private Value[] getUpdateValues(FieldsCollection row, Value userId) {
		List<Value> values = new ArrayList<Value>();
		String timeStamp = DbDriver.getTimeStamp();
		/*
		 * we have two buffers for insert as fields are to be inserted at two
		 * parts
		 */
		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(this.tableName).append(" SET ");

		boolean isFirstField = true;
		for (Field field : this.fields) {
			if (field.fieldType == FieldType.MODIFIED_BY_USER) {
				this.updateValSql(userId, values, timeStamp, update, isFirstField, field);
				isFirstField = false;
				continue;
			}
			if (field.fieldType == FieldType.MODIFIED_TIME_STAMP) {
				this.updateValSql(userId, values, timeStamp, update, isFirstField, field);
				isFirstField = false;
				continue;
			}
			/*
			 * some fields are not updatable
			 */
			if (field.canUpdate()) {
				if (!row.hasValue(field.getName())) {
					continue;
				}
				Value value = field.getValue(row);
				if (Value.isNull(value)) {
					if (field.isNullable) {
						value = Value.newUnknownValue(field.getValueType());
					} else {
						throw new ApplicationError("Column " + field.externalName + " in table " + this.tableName
								+ " is designed to be non-null, but a row is being updated with a null value in it.");
					}
				}
				if (field.isEncrypted) {
					value = this.crypt(value, false);
				}
				this.updateValSql(value, values, timeStamp, update, isFirstField, field);
				isFirstField = false;
			}

		}
		/*
		 * where clause of delete and update are same, but they are valid only
		 * if we have a primary key
		 */

		for (Field field : this.allPrimaryKeys) {
			values.add(row.getValue(field.getName()));
		}

		if (this.allPrimaryKeys != null) {
			if (this.useTimestampForConcurrency) {
				String clause = " AND " + this.modifiedStampField.externalName + "=?";
				this.updateSql = update.append(this.primaryWhereClause).append(clause).toString();
				if (!row.hasValue(this.modifiedStampField.getName())) {
					throw new ApplicationError("Timestamp field for concurrency is required "
							+ this.modifiedStampField.getName() + " is not available ");
				}
				values.add(row.getValue(this.modifiedStampField.getName()));
			} else {
				this.updateSql = update.append(this.primaryWhereClause).toString();
			}
		}

		return values.toArray(new Value[values.size()]);

	}

	private void updateValSql(Value value, List<Value> values, String timeStamp, StringBuilder update,
			boolean isFirstField, Field field) {

		if (!isFirstField) {
			update.append(COMMA);
		}
		update.append(field.externalName).append(Record.EQUAL);
		if (field.fieldType == FieldType.MODIFIED_TIME_STAMP) {
			update.append(timeStamp);
		} else {
			update.append(Record.PARAM);
			values.add(value);
		}
	}

	private void setPrimaryWhere() {
		StringBuilder where = new StringBuilder(" WHERE ");

		boolean firstTime = true;
		for (Field field : this.allPrimaryKeys) {
			if (firstTime) {
				firstTime = false;
			} else {
				where.append(" AND ");
			}
			where.append(field.externalName).append(Record.EQUAL).append(Record.PARAM);
		}

		this.primaryWhereClause = where.toString();
	}

	private void setListSql() {
		Field field = this.getField(this.listFieldName);
		if (field == null) {
			this.invalidFieldName(this.listFieldName);
			return;
		}
		StringBuilder sbf = new StringBuilder();
		sbf.append("SELECT ");
		/*
		 * if this record has no primary key at all, or the listFieldName itself
		 * is the key, then we are to select just the lustField.
		 */
		if (this.allPrimaryKeys == null || this.listFieldName.equals(this.allPrimaryKeys[0].name)) {
			this.valueListTypes = new ValueType[1];
			this.valueListTypes[0] = field.getValueType();
		} else {
			/*
			 * we have to select the primary key and the list field
			 */
			Field keyField = this.allPrimaryKeys[0];
			sbf.append(keyField.externalName).append(" id,");
			this.valueListTypes = new ValueType[2];
			this.valueListTypes[0] = keyField.getValueType();
			this.valueListTypes[1] = field.getValueType();
		}
		sbf.append(field.externalName).append(" value from ").append(this.tableName);
		if (this.listGroupKeyName != null) {
			field = this.getField(this.listGroupKeyName);
			if (field == null) {
				this.invalidFieldName(this.listGroupKeyName);
				return;
			}
			sbf.append(" WHERE ").append(field.externalName).append(EQUAL_PARAM);
			this.valueListKeyType = field.getValueType();
		}
		this.listSql = sbf.toString();
	}

	private void setSuggestSql() {
		Field field = this.getField(this.suggestionKeyName);
		if (field == null) {
			this.invalidFieldName(this.suggestionKeyName);
			return;
		}
		if (this.suggestionOutputNames == null || this.suggestionOutputNames.length == 0) {
			throw new ApplicationError(
					"Record " + this.getQualifiedName() + " specifies suggestion key but no suggestion output fields");
		}
		StringBuilder sbf = new StringBuilder();
		sbf.append("SELECT ");
		for (String fieldName : this.suggestionOutputNames) {
			Field f = this.getField(fieldName);
			if (f == null) {
				this.invalidFieldName(this.suggestionKeyName);
				return;
			}
			sbf.append(f.externalName).append(' ').append(f.name).append(COMMA);
		}
		sbf.setLength(sbf.length() - 1);
		sbf.append(" from ").append(this.tableName).append(" WHERE ").append(field.externalName).append(" LIKE ?");
		this.suggestSql = sbf.toString();
	}

	/**
	 * get list of values, typically for drop-down control
	 *
	 * @param keyValue
	 * @param driver
	 * @param userId
	 * @return sheet that has the data
	 */
	public DataSheet list(String keyValue, DbDriver driver, Value userId) {
		Value[] values = null;
		if (this.listGroupKeyName != null) {
			if (keyValue == null || keyValue.length() == 0) {
				return null;
			}
			values = new Value[1];
			values[0] = Value.parseValue(keyValue, this.valueListKeyType);
		}
		DataSheet sheet = null;
		if (this.okToCache) {
			sheet = this.getListFromCache(keyValue);
			if (sheet != null) {
				return sheet;
			}
		}
		if (this.valueListTypes.length == 1) {
			sheet = new MultiRowsSheet(SINGLE_HEADER, this.valueListTypes);
		} else {
			sheet = new MultiRowsSheet(DOUBLE_HEADER, this.valueListTypes);
		}
		driver.extractFromSql(this.listSql, values, sheet, false);
		if (this.okToCache) {
			this.cacheList(keyValue, sheet);
		}
		return sheet;
	}

	/**
	 * extract rows matching/starting with supplied chars. Typically for a
	 * suggestion list
	 *
	 * @param keyValue
	 * @param matchStarting
	 * @param driver
	 * @param userId
	 * @return sheet that has the data
	 */
	public DataSheet suggest(String keyValue, boolean matchStarting, DbDriver driver, Value userId) {
		String text = keyValue + DbDriver.LIKE_ANY;
		if (!matchStarting) {
			text = DbDriver.LIKE_ANY + text;
		}
		Value[] values = new Value[1];
		values[0] = Value.newTextValue(text);
		DataSheet sheet = this.createSheet(this.suggestionOutputNames, false, false);
		driver.extractFromSql(this.suggestSql, values, sheet, false);
		return sheet;
	}

	/** @return all fields of this record. */
	public Field[] getFields() {
		return this.fields;
	}

	/** @return all fields mapped by their names */
	public Map<String, Field> getFieldsMap() {
		Map<String, Field> map = new HashMap<String, Field>();
		for (Field field : this.fields) {
			map.put(field.name, field);
		}
		return map;
	}

	/**
	 * field name specified at record level is not defined as a field
	 *
	 * @param fieldName
	 */
	private void invalidFieldName(String fieldName) {
		throw new ApplicationError(
				fieldName + " is specified as a field in record " + this.name + " but that field is not defined.");
	}

	/**
	 * extract and validate a data sheet
	 *
	 * @param inData
	 * @param names
	 *            leave it null if you want all fields
	 * @param purpose
	 * @param saveActionExpected
	 * @param ctx
	 * @return data sheet, or null in case of validation errors
	 */
	public DataSheet extractSheet(String[][] inData, String[] names, DataPurpose purpose, boolean saveActionExpected,
			ServiceContext ctx) {
		Field[] fieldsToExtract = this.getFieldsToBeExtracted(names, purpose, saveActionExpected);
		int nbrFields = fieldsToExtract.length;
		/*
		 * array index in the input rows with matching name for this field. -1
		 * if input has no column for this field
		 */
		int[] indexes = new int[nbrFields];
		String[] header = inData[0];
		ValueType[] types = new ValueType[nbrFields];
		String[] allNames = new String[nbrFields];
		/*
		 * set values for inputFields, types and indexes
		 */
		int idx = 0;
		for (Field field : fieldsToExtract) {
			String fn = field.name;
			int map = -1;
			for (int j = 0; j < header.length; j++) {
				if (fn.equals(header[j])) {
					map = j;
					break;
				}
			}
			indexes[idx] = map;
			types[idx] = field.getValueType();
			allNames[idx] = fn;
			idx++;
		}

		if (inData.length == 1) {
			/*
			 * only header, no data. Let us also create a sheet with no data.
			 */
			return new MultiRowsSheet(fieldsToExtract);
		}

		boolean fieldsAreOptional = purpose == DataPurpose.SUBSET;
		/*
		 * we are all set to extract data from each row now
		 */
		List<Value[]> values = new ArrayList<Value[]>();
		for (int i = 1; i < inData.length; i++) {
			String[] inputRow = inData[i];
			Value[] outputRow = new Value[nbrFields];
			for (int j = 0; j < fieldsToExtract.length; j++) {
				Field field = fieldsToExtract[j];
				int map = indexes[j];
				String textValue = map == -1 ? "" : inputRow[map];
				Value value = field.parseText(textValue, fieldsAreOptional, ctx);
				if (value == null) {
					value = Value.newUnknownValue(field.getValueType());
				}
				outputRow[j] = value;
			}
			values.add(outputRow);
		}
		DataSheet ds = new MultiRowsSheet(allNames, values);
		if (this.hasInterFieldValidations) {
			for (FieldsCollection row : ds) {
				for (Field field : fieldsToExtract) {
					field.validateInterfield(row, this.name, ctx);
				}
			}
		}
		return ds;
	}

	/**
	 * extract all fields or named fields for the given purpose
	 *
	 * @param inData
	 * @param namesToExtract
	 *            null if all fields are to be extract
	 * @param extractedValues
	 * @param purpose
	 *            what fields are extracted depends on the purpose
	 * @param saveActionExpected
	 * @param ctx
	 * @return number of fields extracted
	 */
	public int extractFields(Map<String, String> inData, String[] namesToExtract, FieldsCollection extractedValues,
			DataPurpose purpose, boolean saveActionExpected, ServiceContext ctx) {
		/*
		 * is the caller providing a list of fields? else we use all
		 */
		Field[] fieldsToExtract = this.getFieldsToBeExtracted(namesToExtract, purpose, saveActionExpected);
		if (purpose == DataPurpose.FILTER) {

			logger.info("Extracting filter fields");

			return this.extractFilterFields(inData, extractedValues, fieldsToExtract, ctx);
		}
		int result = 0;
		boolean fieldsAreOptional = purpose == DataPurpose.SUBSET;
		for (Field field : fieldsToExtract) {
			String text = inData.get(field.name);
			Value value = field.parseText(text, fieldsAreOptional, ctx);
			if (value != null) {
				extractedValues.setValue(field.name, value);
				result++;
			}
		}
		if (this.hasInterFieldValidations && fieldsAreOptional == false && result > 1) {
			for (Field field : fieldsToExtract) {
				field.validateInterfield(extractedValues, this.name, ctx);
			}
		}
		return result;
	}

	/**
	 * get fields based on names, or all fields
	 *
	 * @param names
	 *            null if all fields are to be extracted
	 * @param purpose
	 * @param extractSaveAction
	 * @return fields
	 */
	public Field[] getFieldsToBeExtracted(String[] names, DataPurpose purpose, boolean extractSaveAction) {
		/*
		 * are we being dictated as to what fields to be used?
		 */
		if (names != null) {
			Field[] result = new Field[names.length];
			int i = 0;
			for (String s : names) {
				Field field = this.indexedFields.get(s);
				if (field == null) {
					if (s.equals(TABLE_ACTION_FIELD_NAME)) {
						field = TABLE_ACTION_FIELD;
					} else {
						throw new ApplicationError(
								s + " is not a valid field in Record " + this.name + ". Field can not be extracted.");
					}
				}
				result[i] = field;
				i++;
			}
			return result;
		}
		/*
		 * we pick fields based on specific purpose
		 */
		if (purpose == DataPurpose.READ) {
			return this.allPrimaryKeys;
		}

		if (extractSaveAction == false) {
			return this.fields;
		}
		/*
		 * append save action as well
		 */
		Field[] result = new Field[this.fields.length + 1];
		int i = 0;
		for (Field field : this.fields) {
			result[i] = field;
			i++;
		}
		result[i] = TABLE_ACTION_FIELD;
		return result;
	}

	/**
	 * filter fields are special fields that have comparators etc..
	 *
	 * @param inData
	 * @param extractedValues
	 * @param fieldsToExtract
	 * @return
	 */
	private int extractFilterFields(Map<String, String> inData, FieldsCollection extractedValues,
			Field[] fieldsToExtract, ServiceContext ctx) {
		int result = 0;
		for (Field field : fieldsToExtract) {
			result += field.parseFilter(inData, extractedValues, ctx);
		}
		/*
		 * some additional fields for filter, like sort
		 */
		/*
		 * what about sort ?
		 */
		String fieldName = ServiceProtocol.SORT_COLUMN_NAME;
		String textValue = inData.get(fieldName);
		if (textValue != null) {
			Value value = ComponentManager.getDataType(DataType.ENTITY_LIST).parseValue(textValue);
			if (value == null) {
				ctx.addMessage(
						new FormattedMessage(Messages.INVALID_ENTITY_LIST, this.defaultSheetName, fieldName, null, 0));
			} else {
				extractedValues.setValue(fieldName, value);
			}
		}

		fieldName = ServiceProtocol.SORT_ORDER;
		textValue = inData.get(fieldName);
		if (textValue != null) {
			textValue = textValue.toLowerCase();
			if (textValue.equals(ServiceProtocol.SORT_ORDER_ASC) || textValue.equals(ServiceProtocol.SORT_ORDER_DESC)) {
				extractedValues.setValue(fieldName, Value.newTextValue(textValue));
			} else {
				ctx.addMessage(
						new FormattedMessage(Messages.INVALID_SORT_ORDER, this.defaultSheetName, fieldName, null, 0));
			}
		}
		return result;
	}

	/** @return field names in this record */
	public String[] getFieldNames() {
		return this.fieldNames;
	}

	/**
	 * get a subset of fields.
	 *
	 * @param namesToGet
	 * @return array of fields. ApplicationError is thrown in case any field is
	 *         not found.
	 */
	public Field[] getFields(String[] namesToGet) {
		if (namesToGet == null) {
			return this.fields;
		}
		Field[] result = new Field[namesToGet.length];
		int i = 0;
		for (String s : namesToGet) {
			Field f = this.indexedFields.get(s);
			if (f == null) {
				throw new ApplicationError("Record " + this.getQualifiedName() + " is asked to get a field named " + s
						+ ". Such a field is not defined for this record.");
			}
			result[i] = f;
			i++;
		}
		return result;
	}

	/** @return are there fields with inter-field validations? */
	public boolean hasInterFieldValidations() {
		return this.hasInterFieldValidations;
	}

	/**
	 * @param thisIsSheet
	 *            is this record being output as sheet?
	 * @return list of output which output records are to be added
	 */
	public OutputRecord[] getOutputRecords(boolean thisIsSheet) {
		List<OutputRecord> list = new ArrayList<>();
		this.addOutputRecordsCascaded(list, null, thisIsSheet);
		return list.toArray(new OutputRecord[0]);
	}

	/**
	 * @param recs
	 *            list to which output records are to be added
	 * @param parentSheetName
	 * @param parentKey
	 */
	private void addOutputRecordsCascaded(List<OutputRecord> recs, Record parentRec, boolean thisIsSheet) {

		OutputRecord outRec = null;
		String thisSheetName = thisIsSheet ? this.defaultSheetName : null;
		outRec = new OutputRecord(thisSheetName, thisSheetName, this.getQualifiedName(), false,
				DataStructureType.ARRAY);
		if (parentRec != null) {
			String parentSheetName = parentRec.getDefaultSheetName();
			Field[] parentKeys = parentRec.allPrimaryKeys;
			Field[] refKeys = this.allParentKeys;
			if (parentKeys == null || refKeys == null || parentKeys.length != refKeys.length) {
				throw new ApplicationError("Parent record " + parentRec.getQualifiedName()
						+ " defines number of children to be read. This would work only if this record defines key field(s) and the child records define corresponding links using parentKeyFields");
			}
			int nbr = parentKeys.length;

			String[] pk = new String[nbr];
			String[] rk = new String[nbr];
			for (int i = 0; i < pk.length; i++) {
				pk[i] = parentKeys[i].name;
				rk[i] = refKeys[i].name;
			}
			outRec.linkToParent(parentSheetName, rk, pk);

		}
		recs.add(outRec);
		if (this.childrenToBeRead == null) {
			return;
		}
		/*
		 * child sheets are hierarchical only if this is output as sheet
		 */
		Record par = thisIsSheet ? this : null;
		for (String child : this.childrenToBeRead) {
			Record cr = ComponentManager.getRecord(child);
			cr.addOutputRecordsCascaded(recs, par, true);
		}
	}

	/**
	 * @param parentSheetName
	 *            if this sheet is to be output as a child. null if a normal
	 *            sheet
	 * @return output record that will copy data sheet to output
	 */
	public InputRecord getInputRecord(String parentSheetName) {
		if (parentSheetName == null) {
			return new InputRecord(this.getQualifiedName(), this.defaultSheetName);
		}
		int nbr = this.allParentKeys.length;
		String[] thisKeys = new String[nbr];
		String[] linkKeys = new String[nbr];
		for (int i = 0; i < this.allParentKeys.length; i++) {
			Field key = this.allParentKeys[i];
			thisKeys[i] = key.name;
			linkKeys[i] = key.referredField;
		}
		return new InputRecord(this.getQualifiedName(), this.defaultSheetName, parentSheetName, thisKeys, linkKeys);
	}

	/** @return the suggestionKeyName */
	public String getSuggestionKeyName() {
		return this.suggestionKeyName;
	}

	/** @return the suggestionOutputNames */
	public String[] getSuggestionOutputNames() {
		return this.suggestionOutputNames;
	}

	/** @return the valueListKeyName */
	public String getValueListKeyName() {
		return this.listGroupKeyName;
	}

	@Override
	public void validate(ValidationContext vtx) {
		ValidationUtil.validateMeta(vtx, this);
		/*
		 * validate fields, and accumulate them in a map for other validations
		 */
		Map<String, Field> fieldMap = new HashMap<String, Field>();
		Map<String, Field> columnMap = new HashMap<String, Field>();
		Set<String> referredFields = new HashSet<String>();
		int nbrKeys = 0;
		Field pkey = null;
		for (Field field : this.fields) {
			/*
			 * validate this field
			 */
			field.validate(vtx, this, referredFields);
			if (this.forFixedWidthRow && field.fieldWidth == 0) {
				vtx.message(new ValidationMessage(field, ValidationMessage.SEVERITY_ERROR, "Field " + field.name
						+ " should specify fieldWidth since the record is designed for a fixed-width row.",
						"fieldWidth"));
			}
			if (FieldType.isPrimaryKey(field.fieldType)) {
				pkey = field;
				nbrKeys++;
			}

			/*
			 * look for duplicate field name
			 */
			if (fieldMap.containsKey(field.name)) {
				vtx.message(new ValidationMessage(field, ValidationMessage.SEVERITY_ERROR, "Field " + field.name +
						" is a duplicate", "name"));
			} else {
				fieldMap.put(field.name, field);
			}

			/*
			 * duplicate column name?
			 */
			if (field.fieldType != FieldType.TEMP) {
				String colName = field.getExternalName();
				if (columnMap.containsKey(colName)) {
					vtx.message(new ValidationMessage(field, ValidationMessage.SEVERITY_ERROR,
							"Field " + field.name + " has its column name set to " +
									colName + ". This column name is duplicated",
							"externalName"));
				} else {
					columnMap.put(colName, field);
				}
			}
		}
		/*
		 * we can generate key, but only if it is of integral type
		 */
		if (this.keyToBeGenerated) {
			if (pkey == null) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"Primary key is not defined but keyToBeGenerated is set to true.", "keyToBeGenerated"));
			} else if (nbrKeys > 0) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"keyToBeGenerated is set to true, but primary key is a composite of more than one fields.",
						"keyToBeGenerated"));
			}
		}

		/*
		 * we can manage concurrency, but only if a time-stamp field is defined
		 */
		if (this.useTimestampForConcurrency) {
			if (this.modifiedStampField == null) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"useTimestampForConcurrency=true, but no field of type modifiedAt.",
						"useTimestampForConcurrency"));
			}
		}
		/*
		 * add referred fields
		 */
		if (this.listFieldName != null) {
			referredFields.add(this.listFieldName);
		}
		if (this.listGroupKeyName != null) {
			referredFields.add(this.listGroupKeyName);
		}
		if (this.suggestionKeyName != null) {
			referredFields.add(this.suggestionKeyName);
		}
		if (this.suggestionOutputNames != null) {
			for (String sug : this.suggestionOutputNames) {
				referredFields.add(sug);
			}
		}
		if (referredFields.size() > 0) {
			for (String fn : referredFields) {
				if (fieldMap.containsKey(fn) == false) {
					vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
							"Referred field name " + fn + " is not defined for this record", null));
				}
			}
		}
		if (DbDriver.isSetup()) {
			/*
			 * check from db if fields are Ok with the db
			 */
			if (this.recordType != RecordUsageType.STRUCTURE) {
				this.validateTable(columnMap, vtx);
			}
			if (this.schemaName != null && DbDriver.isSchmeaDefined(this.schemaName) == false) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"schemaName is set to " + this.schemaName
								+ " but it is not defined as one of additional schema names in application.xml",
						"schemaName"));
			}
		}
	}

	private void validateTable(Map<String, Field> columnMap, ValidationContext vtx) {
		String nam = this.tableName;
		if (nam == null) {
			nam = this.name;
		}
		DataSheet columns = DbDriver.getTableColumns(null, nam);
		if (columns == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					this.tableName + " is not a valid table/view defined in the data base", "tableName"));
			return;
		}
		int nbrCols = columns.length();
		/*
		 * as of now, we check only if names match. we will do more. refer to
		 * DbDrive.COL_NAMES for sequence of columns in each row of columns data
		 * sheet
		 */
		for (int i = 0; i < nbrCols; i++) {
			Value[] row = columns.getRow(i);
			String colName = row[2].toText();
			/*
			 * we should cross-check value type and size. As of now let us check
			 * for length issues with text fields
			 */
			Field field = columnMap.remove(colName);
			if (field == null) {
				/*
				 * column not in this record. No problems.
				 */
				continue;
			}
			if (field.dataType != null) {
				DataType dt = ComponentManager.getDataTypeOrNull(field.dataType);
				if (dt != null && dt.getValueType() == ValueType.TEXT) {
					int len = (int) ((IntegerValue) row[5]).getLong();
					int dtLen = dt.getMaxLength();
					if (dtLen > len) {
						vtx.message(new ValidationMessage(field, ValidationMessage.SEVERITY_ERROR,
								"Field " + field.name + " allows a length of " + dtLen + " but db allows a max of "
										+ len + " chars",
								"dataType"));
					}
				}
			}
		}
		if (columnMap.size() > 0) {
			for (String key : columnMap.keySet()) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						key + " is not a valid column name in the data base table/view", null));
			}
		}
	}

	@Override
	public ComponentType getComponentType() {
		return MY_TYPE;
	}

	/**
	 * get schema name that the table/view associated with this record is part
	 * of. null if it is part of default schema
	 *
	 * @return null if it is default. Otherwise schema name.
	 */
	public String getSchemaName() {
		return this.schemaName;
	}

	/** @return is it okay to cache the list generated from this record? */
	public boolean getOkToCache() {
		return this.listFieldName != null && this.okToCache;
	}

	/**
	 * @return this record is data object if any of its field is non-primitive
	 */
	public boolean isComplexStruct() {
		return this.isComplexStruct;
	}

	/**
	 * Create an array of struct from json that is suitable to be used as a
	 * stored procedure parameter
	 *
	 * @param array
	 * @param con
	 * @param ctx
	 * @param sqlTypeName
	 * @return Array object suitable to be assigned to the callable statement
	 * @throws SQLException
	 */
	public Array createStructArrayForSp(JSONArray array, Connection con, ServiceContext ctx, String sqlTypeName)
			throws SQLException {
		int nbr = array.length();
		Struct[] structs = new Struct[nbr];
		for (int i = 0; i < structs.length; i++) {
			Object childObject = array.get(i);
			if (childObject == null) {
				continue;
			}
			if (childObject instanceof JSONObject == false) {
				ctx.addMessage(Messages.INVALID_VALUE,
						"Invalid input data structure. we were expecting an object inside the array but got "
								+ childObject.getClass().getSimpleName());
				return null;
			}
			structs[i] = this.createStructForSp((JSONObject) childObject, con, ctx, null);
		}
		return DbDriver.createStructArray(con, structs, sqlTypeName);
	}

	/**
	 * extract data as per data structure from json
	 *
	 * @param json
	 * @param ctx
	 * @param con
	 * @param sqlTypeName
	 * @return a struct that can be set as parameter to a stored procedure
	 *         parameter
	 * @throws SQLException
	 */
	public Struct createStructForSp(JSONObject json, Connection con, ServiceContext ctx, String sqlTypeName)
			throws SQLException {
		int nbrFields = this.fields.length;
		Object[] data = new Object[nbrFields];
		for (int i = 0; i < this.fields.length; i++) {
			Field field = this.fields[i];
			Object obj = json.opt(field.name);
			if (obj == null) {

				logger.info("No value for attribute " + field.name);

				continue;
			}
			/*
			 * array of values
			 */
			if (field.fieldType == FieldType.VALUE_ARRAY) {
				if (obj instanceof JSONArray == false) {
					ctx.addMessage(new FormattedMessage(Messages.INVALID_DATA,
							"Input value for parameter. " + field.name + " is expected to be an array of values."));
					continue;
				}
				Value[] arr = field.parseArray(JsonUtil.toObjectArray((JSONArray) obj), this.name, ctx);
				data[i] = DbDriver.createArray(con, arr, field.sqlTypeName);
				continue;
			}

			/*
			 * struct (record or object)
			 */
			if (field.fieldType == FieldType.RECORD) {
				if (obj instanceof JSONObject == false) {
					ctx.addMessage(new FormattedMessage(Messages.INVALID_DATA,
							"Input value for parameter. " + field.name + " is expected to be an objects."));
					continue;
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				data[i] = childRecord.createStructForSp((JSONObject) obj, con, ctx, field.sqlTypeName);
				continue;
			}

			/*
			 * array of struct
			 */
			if (field.fieldType == FieldType.RECORD_ARRAY) {
				if (obj instanceof JSONArray == false) {
					ctx.addMessage(new FormattedMessage(Messages.INVALID_DATA,
							"Input value for parameter. " + field.name + " is expected to be an array of objects."));
					continue;
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				data[i] = childRecord.createStructArrayForSp((JSONArray) obj, con, ctx, field.sqlTypeName);
				continue;
			}
			/*
			 * simple value
			 */
			Value value = field.parseObject(obj, false, ctx);
			if (value != null) {
				data[i] = value.toObject();
			}
		}
		String nameToUse = sqlTypeName;
		if (nameToUse == null) {
			nameToUse = this.sqlStructName;
		}
		return DbDriver.createStruct(con, data, nameToUse);
	}

	/**
	 * Create a json array from an object returned from an RDBMS
	 *
	 * @param data
	 *            as returned from jdbc driver
	 * @return JSON array
	 */
	public JSONArray createJsonArrayFromStruct(Object data) {
		if (data instanceof Object[][] == false) {
			throw new ApplicationError(
					"Input data from procedure is expected to be Object[][] but we got " + data.getClass().getName());
		}
		return this.toJsonArray((Object[][]) data);
	}

	/**
	 * Create a json Object from an object returned from an RDBMS
	 *
	 * @param data
	 *            as returned from jdbc driver
	 * @return JSON Object
	 */
	public JSONObject createJsonObjectFromStruct(Object data) {
		if (data instanceof Object[] == false) {
			throw new ApplicationError(
					"Input data from procedure is expected to be Object[] but we got " + data.getClass().getName());
		}
		return this.toJsonObject((Object[]) data);
	}

	private JSONArray toJsonArray(Object[][] data) {
		JSONArray array = new JSONArray();
		for (Object[] struct : data) {
			array.put(this.toJsonObject(struct));
		}
		return array;
	}

	private JSONObject toJsonObject(Object[] data) {
		int nbrFields = this.fields.length;
		if (data.length != nbrFields) {
			throw this.getAppError(data.length, null, null, data);
		}

		JSONObject json = new JSONObject();
		for (int i = 0; i < data.length; i++) {
			Field field = this.fields[i];
			Object obj = data[i];
			if (obj == null) {
				json.put(field.name, (Object) null);
				continue;
			}
			/*
			 * array of values
			 */
			if (field.fieldType == FieldType.VALUE_ARRAY) {
				if (obj instanceof Object[] == false) {
					throw this.getAppError(-1, field, " is an array of primitives ", obj);
				}
				json.put(field.name, obj);
				continue;
			}

			/*
			 * struct (record or object)
			 */
			if (field.fieldType == FieldType.RECORD) {
				if (obj instanceof Object[] == false) {
					throw this.getAppError(-1, field, " is a record that expects an array of objects ", obj);
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				json.put(field.name, childRecord.toJsonObject((Object[]) obj));
				continue;
			}

			/*
			 * array of struct
			 */
			if (field.fieldType == FieldType.RECORD_ARRAY) {
				if (obj instanceof Object[][] == false) {
					throw this.getAppError(-1, field, " is an array record that expects an array of array of objects ",
							obj);
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				json.put(field.name, childRecord.toJsonArray((Object[][]) obj));
				continue;
			}
			/*
			 * simple value
			 */
			json.put(field.name, obj);
		}
		return json;
	}

	private ApplicationError getAppError(int nbr, Field field, String txt, Object value) {
		StringBuilder sbf = new StringBuilder();
		sbf.append("Error while creating JSON from output of stored procedure using record ")
				.append(this.getQualifiedName()).append(". ");
		if (txt == null) {
			sbf.append("We expect an array of objects with " + this.fields.length + " elements but we got ");
			if (nbr != -1) {
				sbf.append(nbr).append(" elements.");
			} else {
				sbf.append(" an instance of " + value.getClass().getName());
			}
		} else {
			sbf.append("Field ").append(field.name).append(txt).append(" but we got an instance of")
					.append(value.getClass().getName());
		}
		return new ApplicationError(sbf.toString());
	}

	/**
	 * Write an object to writer that represents a JOSONObject for this record
	 *
	 * @param array
	 * @param writer
	 * @throws SQLException
	 * @throws JSONException
	 */
	public void toJsonArrayFromStruct(Object[] array, JSONWriter writer) throws JSONException, SQLException {
		if (array == null) {
			writer.value(null);
			return;
		}
		writer.array();
		for (Object struct : array) {
			this.toJsonObjectFromStruct((Struct) struct, writer);
		}
		writer.endArray();
	}

	/**
	 * Write an object to writer that represents a JOSONObject for this record
	 *
	 * @param struct
	 * @param writer
	 * @throws SQLException
	 * @throws JSONException
	 */
	public void toJsonObjectFromStruct(Struct struct, JSONWriter writer) throws JSONException, SQLException {
		Object[] data = struct.getAttributes();
		int nbrFields = this.fields.length;
		if (data.length != nbrFields) {
			throw this.getAppError(data.length, null, null, data);
		}

		writer.object();
		for (int i = 0; i < data.length; i++) {
			Field field = this.fields[i];
			Object obj = data[i];
			writer.key(field.name);
			if (obj == null) {
				writer.value(null);
				continue;
			}
			/*
			 * array of values
			 */
			if (field.fieldType == FieldType.VALUE_ARRAY) {
				if (obj instanceof Array == false) {
					throw this.getAppError(-1, field, " is an array of primitives ", obj);
				}
				writer.array();
				for (Object val : (Object[]) ((Array) obj).getArray()) {
					writer.value(val);
				}
				writer.endArray();
				continue;
			}

			/*
			 * struct (record or object)
			 */
			if (field.fieldType == FieldType.RECORD) {
				if (obj instanceof Struct == false) {
					throw this.getAppError(-1, field, " is an array of records ", obj);
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				childRecord.toJsonObjectFromStruct((Struct) obj, writer);
				continue;
			}

			/*
			 * array of struct
			 */
			if (field.fieldType == FieldType.RECORD_ARRAY) {
				if (obj instanceof Array == false) {
					throw new ApplicationError("Error while creating JSON from output of stored procedure. Field "
							+ field.name + " is an of record for which we expect an array of object arrays. But we got "
							+ obj.getClass().getName());
				}
				Record childRecord = ComponentManager.getRecord(field.referredRecord);
				Object[] array = (Object[]) ((Array) obj).getArray();
				childRecord.toJsonArrayFromStruct(array, writer);
				continue;
			}
			/*
			 * simple value
			 */
			writer.value(obj);
		}
		writer.endObject();
		return;
	}

	/**
	 * @param ctx
	 * @return an array of values for al fields in this record extracted from
	 *         ctx
	 */
	public Value[] getData(ServiceContext ctx) {
		Value[] result = new Value[this.fields.length];
		for (int i = 0; i < this.fields.length; i++) {
			Field field = this.fields[i];
			result[i] = ctx.getValue(field.name);
		}
		return result;
	}

	/**
	 * crates a default record component for a table from rdbms
	 *
	 * @param schemaName
	 *            null to use default schema. non-null to use that specific
	 *            schema that this table belongs to
	 * @param qualifiedName
	 *            like modulename.recordName
	 * @param tableName
	 *            as in rdbms
	 * @param conversion
	 *            how field names are to be derived from externalName
	 * @param suggester
	 *            data type suggester
	 * @return default record component for a table from rdbms
	 */
	public static Record createFromTable(String schemaName, String qualifiedName, String tableName,
			DbToJavaNameConversion conversion, DataTypeSuggester suggester) {
		DataSheet columns = DbDriver.getTableColumns(schemaName, tableName);
		if (columns == null) {
			String msg = "No table in db with name " + tableName;
			if (schemaName != null) {
				msg += " in schema " + schemaName;
			}

			logger.info(msg);

			return null;
		}
		Record record = new Record();
		record.name = qualifiedName;
		int idx = qualifiedName.lastIndexOf('.');
		if (idx != -1) {
			record.name = qualifiedName.substring(idx + 1);
			record.moduleName = qualifiedName.substring(0, idx);
		}
		record.tableName = tableName;

		int nbrCols = columns.length();
		Field[] fields = new Field[nbrCols];
		for (int i = 0; i < fields.length; i++) {
			Value[] row = columns.getRow(i);
			Field field = new Field();
			fields[i] = field;
			String nam = row[2].toText();
			field.externalName = nam;
			if (conversion == null) {
				field.name = nam;
			} else {
				field.name = conversion.toJavaName(nam);
			}
			String sqlTypeName = row[4].toString();
			field.sqlTypeName = sqlTypeName;

			int sqlType = (int) ((IntegerValue) row[3]).getLong();
			int len = 0;

			if (row[5] != null) {
				len = (int) ((IntegerValue) row[5]).getLong();
			}

			int nbrDecimals = 0;
			if (row[6] != null) {
				nbrDecimals = (int) ((IntegerValue) row[6]).getLong();
			}

			field.dataType = suggester.suggest(sqlType, sqlTypeName, len, nbrDecimals);
			field.isNullable = ((BooleanValue) row[8]).getBoolean();
			field.isRequired = !field.isNullable;
		}
		record.fields = fields;
		return record;
	}

	/**
	 * generate and save draft record.xmls for all tables and views in the rdbms
	 *
	 * @param folder
	 *            where record.xmls are to be saved. Should be a valid folder.
	 *            Created if the path is valid but folder does not exist. since
	 *            we replace any existing file, we recommend that you call with
	 *            a new folder, and then do file copying if required
	 * @param conversion
	 *            how do we form record/field names table/column
	 * @return number of records saved
	 */
	public static int createAllRecords(File folder, DbToJavaNameConversion conversion) {
		if (folder.exists() == false) {
			folder.mkdirs();

			logger.info("Folder created for path " + folder.getAbsolutePath());

		} else if (folder.isDirectory() == false) {

			logger.info(folder.getAbsolutePath() + " is a file but not a folder. Record generation abandoned.");

			return 0;
		}
		String path = folder.getAbsolutePath() + '/';
		DataSheet tables = DbDriver.getTables(null, null);
		if (tables == null) {

			logger.info("No tables in the db. Records not created.");

			return 0;
		}

		logger.info("Found " + tables.length() + " tables for which we are going to create records.");

		DataTypeSuggester suggester = new DataTypeSuggester();
		String[][] rows = tables.getRawData();
		int nbrTables = 0;
		/*
		 * first row is header. Start from second row.
		 */
		for (int i = 1; i < rows.length; i++) {
			String[] row = rows[i];
			String schemaName = row[0];
			if (schemaName != null && schemaName.isEmpty()) {
				schemaName = null;
			}
			String tableName = row[1];
			String recordName = tableName;
			if (conversion != null) {
				recordName = conversion.toJavaName(tableName);
			}

			Record record = Record.createFromTable(schemaName, recordName, tableName, conversion, suggester);
			if (record == null) {

				logger.info("Record " + recordName + " could not be generated from table/view " + tableName);

				continue;
			}
			if (row[2].equals("VIEW")) {
				record.recordType = RecordUsageType.VIEW;
				record.readOnly = true;
			}
			File file = new File(path + recordName + ".xml");
			OutputStream out = null;
			try {
				if (file.exists() == false) {
					file.createNewFile();
				}
				out = new FileOutputStream(file);
				if (XmlUtil.objectToXml(out, record)) {
					nbrTables++;
				}
			} catch (Exception e) {

				logger.error("Record " + recordName + " generated from table/view " + tableName
						+ " but could not be saved. ", e);

				continue;
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (Exception ignore) {
						//
					}
				}
			}
		}
		return nbrTables;
	}

	/** @return table name for this record */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * @param fields
	 */
	public void setFields(Field[] fields) {
		this.fields = fields;
	}

	/**
	 * parses a fixed length text into values for fields.
	 *
	 * @param rowText
	 * @param ctx
	 * @return row of values
	 */
	public Value[] parseRow(String rowText, ServiceContext ctx) {
		Value[] values = new Value[this.fields.length];
		int idx = 0;
		int startAt = 0;
		for (Field field : this.fields) {
			int endAt = startAt + field.fieldWidth;
			String text = rowText.substring(startAt, endAt);
			values[idx] = field.parseText(text, false, ctx);
			idx++;
			startAt = endAt;
		}

		return values;
	}

	/**
	 * @param inRecord
	 *            record to be used to input filter fields
	 * @param inData
	 *            that has the values for filter fields
	 * @param driver
	 * @param useCompactFormat
	 *            json compact format is an array of arrays of data, with first
	 *            row as header. Otherwise, each row is an object
	 * @param writer
	 *            Response writer to which we will output 0 or more objects or
	 *            arrays. (Caller should have started an array. and shoudl end
	 *            array after this call
	 */
	public void filterToJson(Record inRecord, FieldsCollection inData, DbDriver driver, boolean useCompactFormat,
			ResponseWriter writer) {
		/*
		 * we have to create where clause with ? and corresponding values[]
		 */
		SqlAndValues temp = this.getSqlAndValues(inData, inRecord);
		String[] names = this.getFieldNames();
		/*
		 * in compact form, we write a header row values
		 */
		if (useCompactFormat) {
			writer.beginArrayAsArrayElement();
			for (String nam : names) {
				writer.addToArray(nam);
			}
			writer.endArray();
			names = null;
		}
		driver.sqlToJson(temp.sql, temp.values, this.getValueTypes(), names, writer);
	}

	/**
	 * worker method to create a prepared statement and corresponding values for
	 * filter method
	 *
	 * @param inData
	 * @param inRecord
	 * @return struct that has both sql and values
	 */
	private SqlAndValues getSqlAndValues(FieldsCollection inData, Record inRecord) {
		StringBuilder sql = new StringBuilder(this.filterSql);
		List<Value> filterValues = new ArrayList<Value>();
		boolean firstTime = true;
		for (Field field : inRecord.fields) {
			String fieldName = field.name;
			Value value = inData.getValue(fieldName);
			if (Value.isNull(value) || value.toString().isEmpty()) {
				continue;
			}
			if (firstTime) {
				firstTime = false;
			} else {
				sql.append(" AND ");
			}

			FilterCondition condition = FilterCondition.Equal;
			Value otherValue = inData.getValue(fieldName + ServiceProtocol.COMPARATOR_SUFFIX);
			if (otherValue != null && otherValue.isUnknown() == false) {
				condition = FilterCondition.parse(otherValue.toText());
			}

			/** handle the special case of in-list */
			if (condition == FilterCondition.In) {
				Value[] values = Value.parse(value.toString().split(","), field.getValueType());
				/*
				 * we are supposed to have validated this at the input gate...
				 * but playing it safe
				 */
				if (values == null) {
					throw new ApplicationError(value + " is not a valid comma separated list for field " + field.name);
				}
				sql.append(field.externalName).append(" in (?");
				filterValues.add(values[0]);
				for (int i = 1; i < values.length; i++) {
					sql.append(",?");
					filterValues.add(values[i]);
				}
				sql.append(") ");
				continue;
			}

			if (condition == FilterCondition.Like) {
				value = Value.newTextValue(Record.PERCENT + DbDriver.escapeForLike(value.toString()) + Record.PERCENT);
			} else if (condition == FilterCondition.StartsWith) {
				value = Value.newTextValue(DbDriver.escapeForLike(value.toString()) + Record.PERCENT);
			}

			sql.append(field.externalName).append(condition.getSql()).append("?");
			filterValues.add(value);

			if (condition == FilterCondition.Between) {
				otherValue = inData.getValue(fieldName + ServiceProtocol.TO_FIELD_SUFFIX);
				if (otherValue == null || otherValue.isUnknown()) {
					throw new ApplicationError("To value not supplied for field " + this.name + " for filtering");
				}
				sql.append(" AND ?");
				filterValues.add(otherValue);
			}
		}
		Value[] values;
		if (firstTime) {
			/*
			 * no conditions..
			 */
			if (this.okToSelectAll == false) {
				throw new ApplicationError("Record " + this.name
						+ " is likely to contain large number of records, and hence we do not allow select-all operation");
			}
			sql.append(" 1 = 1 ");
			values = new Value[0];
		} else {
			values = filterValues.toArray(new Value[0]);
		}
		/*
		 * is there sort order?
		 */
		Value sorts = inData.getValue(ServiceProtocol.SORT_COLUMN_NAME);
		if (sorts != null) {
			sql.append(" ORDER BY ").append(sorts.toString());
		}
		return new SqlAndValues(sql.toString(), values);
	}

	/**
	 * format a row based on values in the fields collection
	 *
	 * @param outDataFormat
	 * @param fieldValues
	 * @return text that serializes data as per the format
	 */
	public String formatFlatRow(FlatFileRowType outDataFormat, FieldsCollection fieldValues) {
		if (outDataFormat == FlatFileRowType.FIXED_WIDTH) {
			return DataSerializationType.FIXED_WIDTH.serializeFields(fieldValues, this.getFields());
		}
		if (outDataFormat == FlatFileRowType.COMMA_SEPARATED) {
			return DataSerializationType.COMMA_SEPARATED.serializeFields(fieldValues, this.getFields());
		}
		return null;
	}

	/**
	 * extract values from a flat-file text row
	 *
	 * @param inText
	 *            flat-file text row
	 * @param inDataFormat
	 * @param ctx
	 * @return array of values, or null in case of any validation error. Error
	 *         message would have been added to context
	 */
	public Value[] extractFromFlatRow(String inText, FlatFileRowType inDataFormat, ServiceContext ctx) {
		/*
		 * split input into individual text values
		 */
		String[] inputTexts;
		if (inDataFormat == FlatFileRowType.FIXED_WIDTH) {
			inputTexts = this.splitFixedWidthInput(inText, ctx);
			if (inputTexts == null) {
				/*
				 * error: message already added by called method
				 */
				return null;
			}
		} else {
			inputTexts = inText.split(",");
			if (inputTexts.length != this.fields.length) {
				FormattedMessage msg = new FormattedMessage("kernel.invalidInputStream", inText);
				msg.addData(inText);
				ctx.addMessage(msg);
				return null;
			}
		}
		/*
		 * validate and extract
		 */
		Value[] values = new Value[this.fields.length];
		for (int i = 0; i < this.fields.length; i++) {
			Field field = this.fields[i];
			String text = inputTexts[i];
			values[i] = field.parseText(text, false, ctx);
		}
		return values;
	}

	/**
	 * parse fields from a flat file row text into fields, of course with
	 * validation
	 *
	 * @param inText
	 * @param inDataFormat
	 * @param fieldValues
	 *
	 * @param ctx
	 *            any validation errors are added to this
	 */
	public void parseFlatFileRow(String inText, FlatFileRowType inDataFormat, FieldsCollection fieldValues,
			ServiceContext ctx) {
		Value[] values = this.extractFromFlatRow(inText, inDataFormat, ctx);

		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				fieldValues.setValue(this.fieldNames[i], values[i]);
			}
		}
	}

	/**
	 * split fixed-width row text into its field texts
	 *
	 * @param inText
	 * @return
	 */
	private String[] splitFixedWidthInput(String inText, ServiceContext ctx) {
		if (inText.length() < this.minRecordLength) {
			FormattedMessage msg = new FormattedMessage("kernel.invalidInputStream",
					"fixed-width input row has " + inText.length() + " chracters while this record " + this.name
							+ " is designed for a minimum of" + this.minRecordLength + " characters");
			msg.addData(inText);
			ctx.addMessage(msg);
			return null;
		}
		String[] texts = new String[this.fields.length];
		int beginIdx = 0;
		/*
		 * last field takes whatever is available. so keep that out of the loop
		 */
		int nbr = texts.length - 1;
		for (int i = 0; i < nbr; i++) {
			int width = this.fields[i].fieldWidth;
			int endIdx = beginIdx + width;
			texts[i] = inText.substring(beginIdx, endIdx);
			beginIdx = endIdx;
		}
		texts[nbr] = inText.substring(beginIdx);
		return texts;
	}

	/**
	 *
	 * @param forRead
	 * @return array of child records as related records that are suitable
	 *         record based actions
	 */
	public RelatedRecord[] getChildRecordsAsRelatedRecords(boolean forRead) {
		String[] children;
		if (forRead) {
			children = this.getChildrenToOutput();
		} else {
			children = this.getChildrenToInput();
		}
		if (children == null) {
			return null;
		}
		RelatedRecord[] recs = new RelatedRecord[children.length];
		int i = 0;
		for (String child : children) {
			RelatedRecord rr = new RelatedRecord(child, TextUtil.getSimpleName(child));
			rr.getReady();
			recs[i++] = rr;
		}
		return recs;
	}

	/**
	 * @param inData
	 * @return
	 */
	private DataSheet getRowFromCache(FieldsCollection values) {
		AppDataCacherInterface cacher = Application.getAppDataCacher();
		if (cacher == null) {
			return null;
		}
		String key1 = this.getCachingKey(values);
		String key2 = this.getSecondaryKey(values);
		Object obj = cacher.get(key1, key2);
		if (obj == null) {
			return null;
		}
		logger.info("Row located in cache for primary key {} and secondary key {}", key1, key2);
		return (DataSheet) obj;
	}

	/**
	 * get a list of rows for this input
	 *
	 * @param values
	 *            required if this.listGroupKeyName. can be null otherwise.
	 * @return data sheet that is retrieved from cache. null if not found
	 */
	private DataSheet getListFromCache(String groupkeyValue) {
		AppDataCacherInterface cacher = Application.getAppDataCacher();
		if (cacher == null) {
			return null;
		}
		String key = this.getCachingKey(groupkeyValue);
		Object obj = cacher.get(key);
		if (obj == null) {
			return null;
		}
		logger.info("Data sheet located in cache for key {}", key);
		return (DataSheet) obj;
	}

	/**
	 * cache a list of rows
	 *
	 * @param values
	 *            that contains input values. required if this.listGroupKeyName
	 * @param list
	 */
	private void cacheList(String groupKey, DataSheet list) {
		AppDataCacherInterface cacher = Application.getAppDataCacher();
		if (cacher == null) {
			return;
		}
		cacher.put(this.getCachingKey(groupKey), list);
	}

	/**
	 * cache a row for this input
	 *
	 * @param values
	 *            input values
	 * @param row
	 *            output to be cached
	 */
	private void cacheRow(FieldsCollection values, DataSheet row) {
		AppDataCacherInterface cacher = Application.getAppDataCacher();
		if (cacher == null) {
			return;
		}
		cacher.put(this.getCachingKey(values), this.getSecondaryKey(values), row);
	}

	/**
	 * remove all cache for an update using the input values. we invalidate list
	 * as well as all individual cach
	 *
	 * @param values
	 */
	private void invalidateCache(FieldsCollection values) {
		AppDataCacherInterface cacher = Application.getAppDataCacher();
		if (cacher == null) {
			return;
		}
		String groupKey = null;
		if (this.listGroupKeyName != null) {
			groupKey = values.getValue(this.listGroupKeyName).toString();
		}
		for (String recName : this.recordsToBeNotifiedOnChange) {
			Record rec = ComponentManager.getRecord(recName);
			String cacheKey = rec.getCachingKey(groupKey);
			cacher.invalidate(cacheKey);
			cacher.invalidate(cacheKey, null);
		}
	}

	/**
	 * get caching key for a given group key value
	 *
	 * @param groupKeyValue
	 *            null if this record is designed with no group key
	 * @return string to be used as primary/sole key for caching
	 */
	private String getCachingKey(String groupKeyValue) {
		if (this.listGroupKeyName == null) {
			return KEY_PREFIX + this.getQualifiedName();
		}
		return KEY_PREFIX + this.getQualifiedName() + KEY_JOINER + groupKeyValue;
	}

	/**
	 * get caching key by picking up the group key, if required, from the input
	 * values
	 *
	 * @param values
	 * @return string to be used as primary/sole key for caching
	 */
	private String getCachingKey(FieldsCollection inputValues) {
		if (this.listGroupKeyName == null) {
			return KEY_PREFIX + this.getQualifiedName();
		}
		return KEY_PREFIX + this.getQualifiedName() + KEY_JOINER
				+ inputValues.getValue(this.listGroupKeyName).toString();
	}

	/**
	 * get cache key for the key field value(s)
	 *
	 * @param values
	 *            from which key values are extracted
	 * @return string to be used as secondary key for caching.
	 */
	private String getSecondaryKey(FieldsCollection values) {
		StringBuilder result = new StringBuilder();
		for (Field field : this.allPrimaryKeys) {
			/*
			 * we have an extra null at the end. that is OKm so long as we are
			 * consistent
			 */
			result.append(values.getValue(field.getName()).toString()).append(KEY_JOINER);
		}
		return result.toString();
	}

}

class SqlAndValues {
	final String sql;
	final Value[] values;

	SqlAndValues(String sql, Value[] values) {
		this.sql = sql;
		this.values = values;
	}
}
