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
package org.simplity.tp;

import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.SingleRowSheet;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Save (add/modify/delete) a row from a record, and possibly save relevant rows
 * from related records
 *
 * @author simplity.org
 */
public class ReplaceAttachment extends DbAction {
	private static final Logger actionLogger = LoggerFactory.getLogger(ReplaceAttachment.class);

	/** rdbms table that has this column */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.REC)
	String recordName;

	/**
	 * field name of this attachment. This is the name that we refer in our
	 * service context. This is the name that is known to client
	 */
	@FieldMetaData(isRequired = true)
	String attachmentFieldName;

	private String selectSql;
	private String updateSql;
	private String keyFieldName;

	@Override
	protected int doDbAct(ServiceContext ctx, DbDriver driver) {
		Value tokenValue = ctx.getValue(this.attachmentFieldName);
		Value keyValue = ctx.getValue(this.keyFieldName);
		Value[] values = { keyValue };
		String[] columnNames = { this.attachmentFieldName };
		ValueType[] valueTypes = { ValueType.TEXT };
		DataSheet outData = new SingleRowSheet(columnNames, valueTypes);
		int res = driver.extractFromSql(this.selectSql, values, outData, true);
		if (res == 0) {

			actionLogger.info(
					"No row found while reading from record "
							+ this.recordName
							+ " for key value "
							+ keyValue
							+ " and hence no update.");

			return 0;
		}
		/*
		 * save this token into fieldNameOld. Service will take care of removing
		 * this on exit
		 */
		ctx.setValue(
				this.attachmentFieldName + ServiceProtocol.OLD_ATT_TOKEN_SUFFIX, outData.getRow(0)[0]);

		/*
		 * update row with new value
		 */
		Value[] updateValues = { tokenValue, keyValue };
		return driver.executeSql(this.updateSql, updateValues, false);
	}

	@Override
	public DbAccessType getDataAccessType() {
		return DbAccessType.READ_WRITE;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.tp.Action#getReady(int)
	 */
	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		this.createSqls();
	}

	private void createSqls() {
		Record record = ComponentManager.getRecord(this.recordName);
		this.keyFieldName = record.getPrimaryKeyFields()[0].getName();

		String tableName = record.getTableName();
		String attColName = record.getField(this.attachmentFieldName).getExternalName();
		String keyColName = record.getField(this.keyFieldName).getExternalName();

		this.selectSql = "SELECT " + attColName + " FROM " + tableName + " WHERE " + keyColName + " =?";
		this.updateSql = "UPDATE " + tableName + " SET " + attColName + " = ? " + " WHERE " + keyColName + " =?";
	}
}
