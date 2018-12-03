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
package org.simplity.tp;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.db.StoredProcedure;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * execute a stored procedure
 *
 * @author simplity.org
 */
public class ExecuteSp extends DbAction {
	private static final Logger actionLogger = LoggerFactory.getLogger(ExecuteSp.class);

	/** qualified name */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.SP)
	String procedureName;
	/**
	 * sheet name for input data. Null implies that input, if any, would be from
	 * fields collection of ctx
	 */
	String sheetNameForInputParameters;

	/**
	 * output parameters from this SP may be extracted out to a sheet. Else, if
	 * present, they will be extracted to fields collection of ctx
	 */
	String sheetNameForOutputParameters;

	/**
	 * if this procedure has defined outputRecordNames, then you may specify the
	 * sheet names. Default is to use definition from record.
	 */
	String[] outputSheetNames;

	@Override
	protected int doDbAct(ServiceContext ctx, DbDriver driver) {
		FieldsCollection inSheet = ctx;
		FieldsCollection outSheet = ctx;

		if (this.sheetNameForInputParameters != null) {
			inSheet = ctx.getDataSheet(this.sheetNameForInputParameters);
			if (inSheet == null) {
				throw new ApplicationError(
						"Store Procedure Action "
								+ this.actionName
								+ " requires data sheet "
								+ this.sheetNameForInputParameters
								+ " for its input parameters.");
			}
		}

		if (this.sheetNameForOutputParameters != null) {
			outSheet = ctx.getDataSheet(this.sheetNameForOutputParameters);
			if (outSheet == null) {
				throw new ApplicationError(
						"Store Procedure Action "
								+ this.actionName
								+ " requires data sheet "
								+ this.sheetNameForOutputParameters
								+ " for its output parameters.");
			}
		}

		StoredProcedure sp = ComponentManager.getStoredProcedure(this.procedureName);
		DataSheet[] outSheets = sp.execute(inSheet, outSheet, driver, ctx);
		if (outSheets == null) {

			actionLogger.info("Stored procedure " + this.actionName + " execution completed with no sheets.");

			return 1;
		}

		int nbrOutSheets = outSheets.length;

		actionLogger.info(
				"Stored procedure action "
						+ this.actionName
						+ " returned "
						+ nbrOutSheets
						+ " sheets of data");

		String[] names = null;
		if (this.outputSheetNames != null) {
			if (this.outputSheetNames.length != nbrOutSheets) {
				throw new ApplicationError(
						"Store Procedure Action "
								+ this.actionName
								+ " uses stored procedure "
								+ this.procedureName
								+ " with "
								+ this.outputSheetNames.length
								+ " output sheets, but the stored procedure requires "
								+ nbrOutSheets);
			}
			names = this.outputSheetNames;
		} else {
			names = sp.getDefaultSheetNames();
		}
		for (int i = 0; i < nbrOutSheets; i++) {
			ctx.putDataSheet(names[i], outSheets[i]);
		}
		return nbrOutSheets;
	}

	@Override
	public DbAccessType getDataAccessType() {
		return DbAccessType.READ_WRITE;
	}
}
