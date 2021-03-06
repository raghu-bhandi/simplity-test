/*
 * Copyright (c) 2015 EXILANT Technologies Private Limited (www.exilant.com)
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

import java.util.Map;
import java.util.Set;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.FilterCondition;
import org.simplity.kernel.FormattedMessage;
import org.simplity.kernel.Messages;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.comp.ValidationUtil;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.dt.DataType;
import org.simplity.kernel.expr.BinaryOperator;
import org.simplity.kernel.expr.InvalidOperationException;
import org.simplity.kernel.value.BooleanValue;
import org.simplity.kernel.value.InvalidValueException;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.sa.ResponseWriter;
import org.simplity.service.DataStructureType;
import org.simplity.service.OutputRecord;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a basic unit of data. Like unitPrice, dateOfBirth. This is used in
 * different contexts, like a column of a table, a filed in a page etc..
 */
public class Field {
	private static final Logger logger = LoggerFactory.getLogger(Field.class);

	// private static final String DISPLAY_TYPE = "displayType";

	// private static final String LIST_SERVICE_ID = "listServiceId";

	/** identifier */
	@FieldMetaData(isRequired = true)
	String name = null;

	/** Type of column, if this record is associated with a table */
	FieldType fieldType = FieldType.DATA;

	/** * data type as described in dataTypes.xml */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.DT)
	String dataType;

	/**
	 * If this is a column in the database, and we use a different naming
	 * convention for db, this is the way to map field names to column names.
	 * Mapped to input/output names in case this is used for input/output.
	 * Defaults to name
	 */
	String externalName;

	/** Can this field be null in the data base? */
	boolean isNullable;

	/**
	 * if fieldType is PARENT_KEY or FOREIGN_KEY, then the table that this
	 * column points to. If the table is a view, then this is the table from
	 * which this column is picked up from
	 */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.REC)
	String referredRecord;

	/**
	 * Valid only of the table is a view. Use this if the referred column name
	 * is different from this name. Defaults to this name.
	 */
	String referredField;

	/**
	 * * If this field can take set of design-time determined values, this is
	 * the place. this is of the form
	 * "internalValue1:displayValue1,internaValue2,displayValue2......."
	 */
	String valueList;

	/**
	 * * Is a non-null (non-empty) value required in this field? If this is
	 * true, we use default value. If a field is not required, and is part of a
	 * db, then the column in the db is either nullable, or has a default value
	 * set by the db.
	 */
	boolean isRequired = false;

	/**
	 * value to be used if it is not supplied, even if it is optional. use this
	 * if it is known at design time. if this is a deployment/runtime parmeter,
	 * then use the other attribute.
	 */
	String defaultValue = null;

	/**
	 * name of the run-time/deployment-time parameter whoe value is to be used
	 * as default value. This parameter is tried before considering defaultValue
	 */
	String defaultValueParameter = null;

	/**
	 * * is this field mandatory but only when value for another field is
	 * supplied?
	 */
	String basedOnField = null;

	/**
	 * relevant if basedOnField is true. this field is required if the other
	 * field has a specific value (not just any value)
	 */
	String basedOnFieldValue = null;

	/**
	 * At times, we have two fields but only one of them should have value. Do
	 * you have such a pair? If so, one of them should set this. Note that it
	 * does not imply that one of them is a must. It only means that both cannot
	 * be specified. Both can be optional is implemented by isOptional for both.
	 */
	String otherField = null;
	/**
	 * * is this a to-field for another field? Specify the name of the from
	 * field. Note that you should not specify this on both from and to fields.
	 */
	String fromField = null;

	/**
	 * * is this part of a from-to field, and you want to specify that thru this
	 * field?
	 */
	String toField = null;

	/*
	 * following attributes are used by clients for creating good UI
	 */
	/**
	 * * message or help text that is to be flashed to user on the client as a
	 * help text and/or error text when the field value is in error. This
	 * defaults to recordName.fieldName so that a project can have some utility
	 * to maintain all messages for field errors
	 */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.MSG)
	String messageName = null;

	/** label to be used for rendering this field */
	String label;

	/** description is used as help text or validation text */
	String description;

	/**
	 * if this field maps to a non-primitive type, then we need to know the name
	 * by which this is defined in the rdbms for us to use this as a parameter
	 * for stored procedure. If this field is an array of struct, then set this
	 * to the name used by rdbms for the array of struct.
	 */
	String sqlTypeName;

	/**
	 * if the record is mapped to a fixed-width flat file row, then this is the
	 * number of characters this field is taking-up
	 */
	int fieldWidth;

	/** is this field encrypted */
	boolean isEncrypted;

	/**
	 * most application designs have the concept of using code-and-description.
	 * Tagging a field helps in automating its validation, based on actual app
	 * design. For example customer-type field in customer table may be tagged
	 * as common code "custType", and at run time, value of this field is
	 * validated against valid values for "custType"
	 */
	String commonCodeType;

	/*
	 * fields that are cached for performance
	 */
	/** data type object cached for performance */
	private DataType dataTypeObject = null;

	/** default values is parsed into a Value object for performance */
	private Value defaultValueObject = null;

	/** valueList is converted into list of valid values for performance */
	private Map<String, Value> validValues = null;

	/** any of the inter-field validations specified? cached for performance */
	private boolean hasInterFieldValidations = false;

	/** cache the referred field */
	private Field referredFieldCached;

	/*
	 * IMPORTANT : note that we are initializing these true as default
	 * getReady() may change this to false
	 */
	private boolean updateable = true;
	private boolean insertable = true;
	private boolean extractable = true;
	// private boolean doNotShow = false;
	private FieldDisplayType displayType = null;

	/**
	 * let us have a public identity
	 *
	 * @return name of this field. Unique within a record, but can be duplicate
	 *         across records
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return false if this is one of the standard fields that are not to be
	 *         touched retained once inserted
	 */
	public boolean canUpdate() {
		return this.updateable;
	}

	/**
	 * @param dataType
	 */
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * @return false if this is one of the standard fields that are not to be
	 *         touched retained once inserted
	 */
	public boolean canInsert() {
		return this.insertable;
	}

	/**
	 * @return false if this is one of the standard fields that are not to be
	 *         touched retained once inserted
	 */
	public boolean canExtract() {
		return this.extractable;
	}

	/** @return value-type of this field */
	public ValueType getValueType() {
		return this.dataTypeObject.getValueType();
	}

	/** @return field type */
	public FieldType getFieldType() {
		return this.fieldType;
	}

	/** @return data type */
	public DataType getDataType() {
		return this.dataTypeObject;
	}

	/**
	 * @return true if some inter-field validations are defined for this field
	 */
	public boolean hasInterFieldValidations() {
		return this.hasInterFieldValidations;
	}

	/**
	 * parse filter values for this field
	 *
	 * @param inputValues
	 *            source from value for fields to be extracted
	 * @param extratedFields
	 *            into which extracted fields are to be added to
	 * @param ctx
	 * @return number of fields extracted
	 */
	public int parseFilter(Map<String, String> inputValues, FieldsCollection extratedFields, ServiceContext ctx) {

		String textValue = inputValues.get(this.name);
		if (textValue == null) {
			return 0;
		}
		textValue = textValue.trim();
		if (textValue.length() == 0) {
			return 0;
		}
		/*
		 * what is the comparator
		 */
		String otherName = this.name + ServiceProtocol.COMPARATOR_SUFFIX;
		String otherValue = inputValues.get(otherName);
		FilterCondition f = FilterCondition.parse(otherValue);

		Value value = null;
		ValueType vt = this.getValueType();
		/*
		 * handle the special case of in list
		 */
		if (FilterCondition.In == f) {
			if (vt != ValueType.TEXT && vt != ValueType.INTEGER && vt != ValueType.DECIMAL) {

				ctx.addMessage(new FormattedMessage(Messages.INVALID_VALUE, this.externalName,
						" inList condition is valid for numeric and text fields only "));
				return 0;
			}
			Value[] vals = Value.parse(textValue.split(","), vt);
			if (vals == null) {
				ctx.addMessage(new FormattedMessage(Messages.INVALID_VALUE, this.name));
				return 0;
			}
			extratedFields.setValue(this.name, Value.newTextValue(textValue));
			extratedFields.setValue(otherName, Value.newTextValue(otherValue));
			return 1;
		}
		/*
		 * filter field need not conform to data-type but it should be of the
		 * same value type
		 *
		 * Special case of in list. We validate it here, but except if it is
		 * IN_LIST operator
		 */
		value = Value.parseValue(textValue, vt);
		if (value == null) {
			ctx.addMessage(new FormattedMessage(Messages.INVALID_VALUE, this.externalName));
		} else {
			extratedFields.setValue(this.name, value);
		}
		if (f == null) {
			extratedFields.setValue(otherName, Value.newTextValue(ServiceProtocol.EQUAL));
			return 0;
		}
		extratedFields.setValue(otherName, Value.newTextValue(otherValue));
		if (f != FilterCondition.Between) {
			return 1;
		}
		otherName = this.name + ServiceProtocol.TO_FIELD_SUFFIX;
		textValue = inputValues.get(otherName);
		value = null;
		if (textValue != null) {
			value = Value.parseValue(textValue, vt);
		}
		if (value == null) {
			ctx.addMessage(new FormattedMessage(Messages.INVALID_VALUE, otherName));
		} else {
			extratedFields.setValue(otherName, value);
		}
		return 1;
	}

	/**
	 * parse and validate input value for this field
	 *
	 * @param inputValue
	 *            to which any validation error is added
	 * @param allFieldsAreOptional
	 *            true if field is to be considered optional irrespective of
	 *            setting at the field level
	 * @param ctx
	 *            can be null if this is used outside of a service execution, in
	 *            which case defaultValueParameter is ignored
	 * @return parsed value. null if no input or input is in error.
	 */
	public Value parseText(String inputValue, boolean allFieldsAreOptional, ServiceContext ctx) {
		String textValue = inputValue == null ? null : inputValue.trim();
		return this.parseObject(textValue, allFieldsAreOptional, ctx);
	}

	/**
	 * parse object as input for this field.
	 *
	 * @param inputValue
	 * @param allFieldsAreOptional
	 *            true if this field is to be considered optional
	 * @param ctx
	 *            can be null if this is used outside of a service execution, in
	 *            which case defaultValueParameter is ignored
	 * @return parsed and validated value. Null if there is no value. Any
	 *         validation error is added to ctx
	 */
	public Value parseObject(Object inputValue, boolean allFieldsAreOptional, ServiceContext ctx) {
		if (this.extractable == false) {
			return null;
		}
		Value value = null;
		if (inputValue != null) {
			value = this.dataTypeObject.getValueType().parseObject(inputValue);
			if (value == null) {
				ctx.addMessage(new FormattedMessage(Messages.INVALID_DATA, this.externalName, inputValue.toString()));
				return null;
			}
		}
		return this.parseValue(value, allFieldsAreOptional, ctx);
	}

	/**
	 * parse object as input for this field.
	 *
	 * @param values
	 * @param recordName
	 * @param ctx
	 * @return parsed and validated value. Null if there is no value. Any
	 *         validation error is added to ctx
	 */
	public Value[] parseArray(Object[] values, String recordName, ServiceContext ctx) {

		if (values == null || values.length == 0) {
			if (this.isRequired) {
				ctx.addMessage(new FormattedMessage(Messages.VALUE_REQUIRED, recordName, this.externalName, null, 0));
			}
			return null;
		}
		Value[] result = new Value[values.length];
		for (int i = 0; i < values.length; i++) {
			Object val = values[i];
			if (val == null) {
				continue;
			}
			Value value = this.parseObject(val, false, ctx);
			if (value != null) {
				result[i] = value;
			}
		}
		return result;
	}

	/**
	 * @param inputValue
	 * @param allFieldsAreOptional
	 * @param ctx
	 *            service context in which this validation is required. can be
	 *            null. if null, any db related validation like common-code may
	 *            not happen
	 * @return value that is parsed. This could be the same as input, or
	 *         modified based on some validation rules.
	 */
	public Value parseValue(Value inputValue, boolean allFieldsAreOptional, ServiceContext ctx) {
		if (this.extractable == false) {
			return null;
		}

		if (inputValue == null || inputValue.isUnknown()) {
			if (this.defaultValueParameter != null) {
				String txt = ParameterRetriever.getValue(this.defaultValueParameter, ctx);
				if (txt != null) {
					return Value.parseValue(txt, this.getValueType());
				}
			}
			if (this.defaultValueObject != null) {
				return this.defaultValueObject;
			}
			if (this.isRequired && allFieldsAreOptional == false) {
				ctx.addMessage(new FormattedMessage(Messages.VALUE_REQUIRED, this.externalName));
			}
			return null;
		}
		Value result = null;
		if (this.validValues != null) {
			result = this.validValues.get(inputValue.toString());
		} else {
			result = this.dataTypeObject.validateValue(inputValue);
		}

		if (result != null && this.commonCodeType != null) {
			if (CommonCodeValidator.isValid(this.commonCodeType, result, ctx) == false) {
				result = null;
			}
		}
		if (result == null) {
			String msgName = this.messageName == null ? Messages.INVALID_DATA : this.messageName;
			ctx.addMessage(new FormattedMessage(msgName, this.externalName, inputValue.toString()));
		}
		return result;
	}

	/**
	 * carry out inter-field validations for this field
	 *
	 * @param fields
	 * @param recordName
	 * @param ctx
	 */
	public void validateInterfield(FieldsCollection fields, String recordName, ServiceContext ctx) {
		if (this.hasInterFieldValidations == false) {
			return;
		}
		Value value = fields.getValue(this.name);
		if (value == null) {
			/*
			 * possible error case 1 : basedOnField forces this field to be
			 * mandatory
			 */
			if (this.basedOnField != null) {
				Value basedValue = fields.getValue(this.basedOnField);
				if (basedValue != null) {
					if (this.basedOnFieldValue == null) {
						ctx.addMessage(new FormattedMessage(Messages.INVALID_BASED_ON_FIELD, recordName, this.name,
								this.basedOnField, 0));
					} else {
						/*
						 * not only that the basedOnField has to have value, it
						 * should have a specific value
						 */
						Value matchingValue = Value.parseValue(this.basedOnFieldValue, basedValue.getValueType());
						if (basedValue.equals(matchingValue)) {
							ctx.addMessage(new FormattedMessage(Messages.INVALID_BASED_ON_FIELD, recordName, this.name,
									this.basedOnField, 0));
						}
					}
				}
			}
			/*
			 * case 2 : other field is not provided. hence this becomes
			 * mandatory
			 */
			if (this.otherField != null) {
				Value otherValue = fields.getValue(this.basedOnField);
				if (otherValue == null) {
					ctx.addMessage(new FormattedMessage(Messages.INVALID_OTHER_FIELD, recordName, this.name,
							this.basedOnField, 0));
				}
			}
			return;
		}

		/*
		 * problems when this field has value - case 1 - from field
		 */
		BooleanValue result;
		if (this.fromField != null) {
			Value fromValue = fields.getValue(this.fromField);
			if (fromValue != null) {
				try {
					result = (BooleanValue) BinaryOperator.Greater.operate(fromValue, value);
				} catch (InvalidOperationException e) {
					throw new ApplicationError("incompatible fields " + this.name + " and " + this.fromField
							+ " are set as from-to fields");
				}
				if (result.getBoolean()) {
					ctx.addMessage(
							new FormattedMessage(Messages.INVALID_FROM_TO, recordName, this.fromField, this.name, 0));
				}
			}
		}
		/*
		 * case 2 : to field
		 */
		if (this.toField != null) {
			Value toValue = fields.getValue(this.toField);
			if (toValue != null) {
				try {
					result = (BooleanValue) BinaryOperator.Greater.operate(value, toValue);
				} catch (InvalidOperationException e) {
					throw new ApplicationError("incompatible fields " + this.name + " and " + this.fromField
							+ " are set as from-to fields");
				}
				if (result.getBoolean()) {
					ctx.addMessage(
							new FormattedMessage(Messages.INVALID_FROM_TO, recordName, this.name, this.toField, 0));
				}
			}
		}
	}

	/** to be called by record after adding all fields */
	void getReady(Record myRecord, Record defRecord, boolean isView) {
		/*
		 * set default type
		 */
		if (this.fieldType == null) {
			this.fieldType = isView ? FieldType.VIEW : FieldType.DATA;
			/*
			 * let us also correct the default type for table and view
			 */
		} else if (this.fieldType == FieldType.DATA) {
			if (isView) {
				this.fieldType = FieldType.VIEW;
			}

		} else if (this.fieldType == FieldType.VIEW) {
			if (!isView) {
				this.fieldType = FieldType.DATA;
			}
		}
		this.resolverReference(myRecord, defRecord);

		if (this.externalName == null) {
			this.externalName = this.name;
		}

		if (this.dataType == null) {
			if (this.isPrimitive() || this.fieldType == FieldType.VALUE_ARRAY) {
				throw new ApplicationError("Field " + this.name
						+ " is not associated with any data type. Please specify dataType attribute, or associate this panel with the right record.");
			}
			/*
			 * for safety, we keep a text data type
			 */
			this.dataType = DataType.DEFAULT_TEXT;

		}
		this.dataTypeObject = ComponentManager.getDataType(this.dataType);

		this.hasInterFieldValidations = this.fromField != null || this.toField != null || this.otherField != null
				|| this.basedOnField != null;
		/*
		 * parse default value
		 */
		if (this.defaultValue != null) {
			this.defaultValueObject = this.dataTypeObject.parseValue(this.defaultValue);
			if (Value.isNull(this.defaultValueObject)) {
				throw new ApplicationError(
						"Field " + this.name + " has an invalid default value of " + this.defaultValue);
			}
			logger.info("{} will be used as default for field {}", this.defaultValueObject, this.name);
		}
		/*
		 * parse value list
		 */
		if (this.valueList == null) {
			this.valueList = this.dataTypeObject.getValueList();
			if (this.valueList != null) {
				this.validValues = this.dataTypeObject.getValidValues();
			}
		} else {
			this.validValues = Value.parseValueList(this.valueList, this.dataTypeObject.getValueType());
		}
		if (this.messageName == null) {
			this.messageName = this.dataTypeObject.getMessageName();
		}
		if (this.description == null) {
			this.description = this.dataTypeObject.getDescription();
		}
		this.setAbles(myRecord);
	}

	private void resolverReference(Record myRecord, Record defaultRefRecord) {
		/*
		 * referred field. Note that both referredField and referredRecord are
		 * optional, and we have to use default values
		 */
		if (this.fieldType == FieldType.RECORD || this.fieldType == FieldType.RECORD_ARRAY) {
			return;
		}

		Record ref = null;
		if (this.referredRecord != null) {
			ref = myRecord.getRefRecord(this.referredRecord);
		} else if (defaultRefRecord != null) {
			ref = defaultRefRecord;
			this.referredRecord = ref.getQualifiedName();
		} else {
			/*
			 * no reference record.
			 */
			return;
		}

		String refName = this.referredField == null ? this.name : this.referredField;
		Field refField = ref.getField(refName);
		if (refField == null) {
			if (this.referredField == null) {
				/*
				 * user did not specify this
				 */
				return;
			}
			throw new ApplicationError("Field " + this.name + " in record " + myRecord.getQualifiedName()
					+ " refers to field " + this.referredField + " of record " + this.referredRecord
					+ ". Referred field is not found in the referred record.");
		}
		if (this.referredField == null) {
			this.referredField = refField.name;
		}
		this.referredFieldCached = refField;
		this.copyFromRefField();
	}

	/** */
	private void setAbles(Record myRecord) {
		switch (this.fieldType) {
		case CREATED_BY_USER:
			this.updateable = false;
			this.extractable = false;
			this.isRequired = false;
			break;
		case CREATED_TIME_STAMP:
			this.insertable = false;
			this.updateable = false;
			this.extractable = false;
			this.isRequired = false;
			break;
		case MODIFIED_BY_USER:
			this.extractable = false;
			break;
		case MODIFIED_TIME_STAMP:
			this.insertable = false;
			this.updateable = false;
			this.isRequired = false;
			break;
		case PARENT_KEY:
			this.updateable = false;
			break;
		case PRIMARY_AND_PARENT_KEY:
		case PRIMARY_KEY:
			this.updateable = false;
			if (myRecord.keyToBeGenerated) {
				this.insertable = false;
				this.isRequired = false;
			}
			break;
		default:
			break;
		}
	}

	/**
	 * @param refField
	 */
	private void copyFromRefField() {
		Field ref = this.referredFieldCached;
		if (this.dataType == null) {
			this.dataType = ref.dataType;
		}
		if (this.valueList == null) {
			this.valueList = ref.valueList;
		}
		if (this.label == null) {
			this.label = ref.label;
		}
		if (this.defaultValue == null) {
			this.defaultValue = ref.defaultValue;
		}
		if (this.defaultValueParameter == null) {
			this.defaultValueParameter = ref.defaultValueParameter;
		}
		if (this.messageName == null) {
			this.messageName = ref.messageName;
		}
	}

	/**
	 * can a value be supplied for this field during an insert operation?
	 *
	 * @return true if value can be supplied, false otherwise
	 */
	public FieldDisplayType getDisplayType() {
		return this.displayType;
	}

	/**
	 * get a simple text field on the fly, with no validation requirement
	 *
	 * @param fieldName
	 * @param valueType
	 * @return simple field with no constraint except the value type
	 */
	public static Field getDefaultField(String fieldName, ValueType valueType) {
		Field field = new Field();
		field.name = fieldName;
		field.dataType = valueType.getDefaultDataType();
		field.dataTypeObject = ComponentManager.getDataType(field.dataType);

		return field;
	}

	/**
	 * @return true if a value for this field is required, false if it is
	 *         optional
	 */
	public boolean isRequired() {
		return this.isRequired;
	}

	/**
	 * @param values
	 * @return get value for the field from the collection. In case it is not
	 *         found, get default
	 */
	public Value getValue(FieldsCollection values) {
		Value value = values.getValue(this.name);

		if (Value.isNull(value)) {
			return this.defaultValueObject;
		}

		if (this.dataType.equals(ValueType.DATE.getDefaultDataType())) {
			if (value.getValueType().equals(ValueType.INTEGER)) {
				try {
					value = Value.newDateValue(value.toInteger());
				} catch (InvalidValueException e) {
					e.printStackTrace();
				}
			}
		}
		return value;
	}

	/**
	 * @param values
	 * @param ctx
	 * @return get value for the field from the collection. In case it is not
	 *         found, get default
	 */
	public Value getValue(FieldsCollection values, ServiceContext ctx) {
		Value value = values.getValue(this.name);

		if (Value.isNull(value)) {
			if (this.defaultValueParameter != null && ctx != null) {
				String txt = ParameterRetriever.getValue(this.defaultValueParameter, ctx);
				if (txt != null) {
					return Value.parseValue(txt, this.getValueType());
				}
			}
			return this.defaultValueObject;
		}

		if (this.dataType.equals(ValueType.DATE.getDefaultDataType())) {
			if (value.getValueType().equals(ValueType.INTEGER)) {
				try {
					value = Value.newDateValue(value.toInteger());
				} catch (InvalidValueException e) {
					e.printStackTrace();
				}
			}
		}
		return value;
	}

	/**
	 * @param ctx
	 * @param record
	 */
	void validate(ValidationContext vtx, Record record, Set<String> referredFields) {
		ValidationUtil.validateMeta(vtx, this);
		/*
		 * add referred fields
		 */
		if (this.fromField != null) {
			referredFields.add(this.fromField);
		}
		if (this.toField != null) {
			referredFields.add(this.toField);
		}
		if (this.otherField != null) {
			referredFields.add(this.otherField);
		}
		if (this.basedOnField != null) {
			referredFields.add(this.fromField);
		}

		if (this.isPrimitive()) {
			return;
		}
		/*
		 * this field refers to a child-record
		 */
		if (this.sqlTypeName == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"field " + this.name
							+ " represents a child-record/array and hence sqlTypeName is mandatory for this field",
					"sqlTypeName"));
		}
		if (this.fieldType != FieldType.VALUE_ARRAY && this.referredRecord == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"field " + this.name + " represents a child-record, but referred record is not specified.", null));
		}
		/*
		 * also, this record has to be a data structure
		 */
		if (record.recordType != RecordUsageType.STRUCTURE) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"field " + this.name
							+ " represents a child-record, but this record is not a data structure. child-record is valid only for data structures.",
					null));
		}
		if (this.fieldType == FieldType.RECORD_ARRAY && record.sqlStructName == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"field " + this.name + " represents an array of child record " + this.referredRecord
							+ ". But that child record has not defined a value for sqlStructName.",
					"referredRecord"));
		}
	}

	/** @return column name of this field */
	public String getExternalName() {
		if (this.externalName != null) {
			return this.externalName;
		}
		return this.name;
	}

	/**
	 * @param externalName
	 */
	public void setExternalName(String externalName) {
		this.externalName = externalName;
	}

	/**
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param sqlTypeName
	 */
	public void setSqlTypeName(String sqlTypeName) {
		this.sqlTypeName = sqlTypeName;
	}

	/**
	 * @param fieldType
	 */
	public void setFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	/**
	 * @param isNullable
	 */
	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	/** @return is this field nullable */
	public boolean isNullable() {
		return this.isNullable;
	}

	/**
	 * @param isRequired
	 */
	public void setRequired(boolean isRequired) {
		this.isRequired = isRequired;
	}

	/** @return record of the field that this field refers to */
	public String getReferredRecord() {
		return this.referredRecord;
	}

	/**
	 * @param referredRecord
	 */
	public void setReferredRecord(String referredRecord) {
		this.referredRecord = referredRecord;
	}

	/** @return field that this field refers to */
	public String getReferredField() {
		return this.referredField;
	}

	/**
	 * @param referredField
	 */
	public void setReferredField(String referredField) {
		this.referredField = referredField;
	}

	/**
	 *
	 * @return true if this field is a primitive type. false if it is a
	 *         record/array
	 */
	public boolean isPrimitive() {
		return FieldType.isPrimitive(this.fieldType);
	}

	/** @return the fieldWidth to be used for fixed-width formatting */
	public int getFieldWidth() {
		return this.fieldWidth;
	}

	/**
	 * @param fieldWidth
	 *            to be used for a fixed-width formatting
	 */
	public void setFieldWidth(int fieldWidth) {
		this.fieldWidth = fieldWidth;
	}

	/**
	 * write this field from context to response writer
	 *
	 * @param writer
	 * @param values
	 */
	public void write(ResponseWriter writer, FieldsCollection values) {
		writer.setField(this.externalName, values.getValue(this.name));
	}

	/**
	 * write this value to the writer
	 *
	 * @param writer
	 * @param value
	 */
	public void write(ResponseWriter writer, Value value) {
		writer.setField(this.externalName, value);
	}

	/**
	 * create an output record for this non-primitive field, based on its
	 * underlying record
	 *
	 * @param outputAs
	 * @return an outputRecord for the specified purpose
	 */
	public OutputRecord getOutputRecord(DataStructureType outputAs) {
		if (this.isPrimitive()) {
			return null;
		}
		OutputRecord rec = new OutputRecord(this.name, this.externalName, this.referredRecord, true, outputAs);
		rec.getReady();
		return rec;
	}
}
