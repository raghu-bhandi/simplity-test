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
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.value.InvalidValueException;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get suggestions from the table associated with a record based on the inputs.
 * This action is "auto-generated" for on-the-fly service, but action is useful
 * for a service that has to do other things as well. This is a thin wrapper on
 * record.suggest()
 *
 * @author simplity.org
 */
public class Suggest extends DbAction {
	private static final Logger actionLogger = LoggerFactory.getLogger(Suggest.class);

	/** record that is to be used */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.REC)
	String recordName;
	/** field to filter on. Defaults to specification in record */
	String fieldToMatch;
	/** name of the output data sheet */
	String outputSheetName;

	/** default */
	public Suggest() {
	}

	/**
	 * suggestion action for the record
	 *
	 * @param record
	 */
	public Suggest(Record record) {
		this.actionName = "suggest" + record.getSimpleName();
		this.recordName = record.getQualifiedName();
	}

	@Override
	protected int doDbAct(ServiceContext ctx, DbDriver driver) {
		Record record = ComponentManager.getRecord(this.recordName);
		Value value = ctx.getValue(this.fieldToMatch);
		if (value == null) {

			actionLogger.info(
					"No value is available in field "
							+ this.fieldToMatch
							+ " for us to suggest. No suggestions sent to client");

			return 0;
		}
		boolean matchStarting = false;
		Value v = ctx.getValue(ServiceProtocol.SUGGEST_STARTING);
		try {
			if (v != null && v.toBoolean()) {
				matchStarting = true;
			}
		} catch (InvalidValueException e) {

			actionLogger.info(
					"we expected boolean value in "
							+ ServiceProtocol.SUGGEST_STARTING
							+ " but encountered "
							+ v
							+ ". Assumed false value.");
		}
		DataSheet sheet = record.suggest(value.toString(), matchStarting, driver, ctx.getUserId());
		if (sheet == null) {
			return 0;
		}
		String sheetName = this.outputSheetName == null ? record.getDefaultSheetName() : this.outputSheetName;
		ctx.putDataSheet(sheetName, sheet);
		return sheet.length();
	}

	@Override
	public DbAccessType getDataAccessType() {
		return DbAccessType.READ_ONLY;
	}

	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		if (this.recordName == null) {
			throw new ApplicationError("Suggest action requires recordName");
		}
		if (this.fieldToMatch == null) {
			this.fieldToMatch = ServiceProtocol.LIST_SERVICE_KEY;
		}
	}
}
