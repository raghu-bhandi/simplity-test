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
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.db.Sql;
import org.simplity.kernel.dm.Record;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read a row/s from as output of a prepared statement/sql
 *
 * @author simplity.org
 */
public class ReadWithSql extends DbAction {
	private static final Logger actionLogger = LoggerFactory.getLogger(ReadWithSql.class);

	/** fully qualified sql name */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.SQL)
	String sqlName;
	/** input sheet name */
	String inputSheetName;
	/** output sheet name */
	String outputSheetName;

	/** any other child records to be read for this record? */
	RelatedRecord[] childRecords;

	/**
	 * should child records for this filter/record be filtered automatically?
	 */
	boolean cascadeFilterForChildren;

	@Override
	protected int doDbAct(ServiceContext ctx, DbDriver driver) {
		Sql sql = ComponentManager.getSql(this.sqlName);
		DataSheet outSheet = null;
		if (this.inputSheetName == null) {
			outSheet = sql.extract(ctx, driver);
		} else {
			DataSheet inSheet = ctx.getDataSheet(this.inputSheetName);
			if (inSheet == null) {
				actionLogger.info(
						"Read Action "
								+ this.actionName
								+ " did not execute because input sheet "
								+ this.inputSheetName
								+ " is not found.");

				return 0;
			}
			outSheet = sql.extract(inSheet, driver);
		}
		/*
		 * did we get any data at all?
		 */
		int nbrRows = outSheet.length();
		if (this.outputSheetName == null) {
			if (nbrRows > 0) {
				ctx.copyFrom(outSheet);
			}
		} else {
			/*
			 * we would put an empty sheet. That is the design, not a bug
			 */
			ctx.putDataSheet(this.outputSheetName, outSheet);
		}

		/*
		 * be a responsible parent :-)
		 */
		if (this.childRecords != null && nbrRows > 0) {
			for (RelatedRecord rr : this.childRecords) {
				Record record = ComponentManager.getRecord(rr.recordName);
				record.filterForParents(outSheet, driver, rr.sheetName, this.cascadeFilterForChildren, ctx);
			}
		}
		return nbrRows;
	}

	@Override
	public DbAccessType getDataAccessType() {
		return DbAccessType.READ_ONLY;
	}

	@Override
	public void validateSpecific(ValidationContext vtx, Service service) {
		super.validateSpecific(vtx, service);

		if (this.childRecords != null) {
			for (RelatedRecord rec : this.childRecords) {
				rec.validate(vtx);
			}
		}
	}
}
