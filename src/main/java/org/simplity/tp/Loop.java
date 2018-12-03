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

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.data.AlreadyIteratingException;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.DataSheetIterator;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loop through a set of actions for each row in a data sheet
 *
 * @author simplity.org
 */
public class Loop extends Block {
	private static final Logger actionLogger = LoggerFactory.getLogger(Loop.class);

	/** data sheet on which to loop */
	String dataSheetName;

	/**
	 * for the loop, do you want to treat some columns as if they are fields in
	 * the collection? This feature helps in re-using services that assume
	 * fields as sub-service inside loops. * for all columns
	 */
	String[] columnsToCopyAsFields;

	/**
	 * in case the code inside the loop is updating some of the fields that are
	 * to be copied back to data sheet
	 */
	String[] fieldsToCopyBackAsColumns;

	/**
	 * in case this service is used as a batch processing kind of logic, then
	 * you may want to use this loop-block as a unit of work, and you are
	 * amenable to stop-work at this point, and not in-between
	 */
	boolean breakOnInterrupt;
	/** special case where we are to copy all columns as fields */
	private boolean copyAllColumnsToFields;

	/** special case where we are to copy back all fields into columns */
	private boolean copyBackAllColumns;

	@Override
	protected Value delegate(ServiceContext ctx, DbDriver driver) {
		BlockWorker actionBlock = new BlockWorker(this.actions, this.indexedActions, ctx, null, false);
		if (this.dataSheetName != null) {
			return this.loopOnSheet(actionBlock, driver, ctx);
		}
		if (this.executeOnCondition == null) {

			actionLogger.info(
					"Loop action "
							+ this.actionName
							+ " has niether data sheet, nor condition. This is a run-for-ever loop,but could be interrupted");
		}
		return this.loopOnCondition(actionBlock, driver, ctx);
	}

	/**
	 * loop over this block for supplied expression/condition
	 *
	 * @param expr
	 * @param driver
	 * @return true if normal completion. False if we encountered a STOP signal
	 */
	private Value loopOnCondition(BlockWorker actionBlock, DbDriver driver, ServiceContext ctx) {
		/*
		 * loop with a condition
		 */
		try {
			Value toContinue = Value.VALUE_TRUE;
			if (this.executeOnCondition != null) {
				toContinue = this.executeOnCondition.evaluate(ctx);
			}
			boolean interrupted = false;
			while (toContinue.toBoolean()) {
				if (this.breakOnInterrupt && Thread.interrupted()) {
					interrupted = true;
					break;
				}
				/*
				 * run the block
				 */
				JumpSignal signal = actionBlock.execute(driver);
				/*
				 * are we to get out?
				 */
				if (signal == JumpSignal.STOP) {
					return Value.VALUE_FALSE;
				}
				if (signal == JumpSignal.BREAK) {
					return Value.VALUE_TRUE;
				}
				/*
				 * should we continue?
				 */
				if (this.executeOnCondition != null) {
					toContinue = this.executeOnCondition.evaluate(ctx);
				}
			}
			if (interrupted) {

				actionLogger.info("Coming out of loop because the thread is interrupted");

				Thread.currentThread().interrupt();
			}
			return Value.VALUE_TRUE;
		} catch (Exception e) {
			throw new ApplicationError(
					e, "Error while evaluating " + this.executeOnCondition + " into a boolean value.");
		}
	}

	/**
	 * loop over this block for supplied data sheet
	 *
	 * @param expr
	 * @param driver
	 * @return true if normal completion. False if we encountered a STOP signal
	 */
	private Value loopOnSheet(BlockWorker actionBlock, DbDriver driver, ServiceContext ctx) {
		DataSheet ds = ctx.getDataSheet(this.dataSheetName);
		if (ds == null) {

			actionLogger.info(
					"Data Sheet "
							+ this.dataSheetName
							+ " not found in the context. Loop action has no work.");

			return Value.VALUE_TRUE;
		}
		if (ds.length() == 0) {

			actionLogger.info("Data Sheet " + this.dataSheetName + " has no data. Loop action has no work.");

			return Value.VALUE_TRUE;
		}
		DataSheetIterator iterator = null;
		try {
			iterator = ctx.startIteration(this.dataSheetName);
		} catch (AlreadyIteratingException e) {
			throw new ApplicationError(
					"Loop action is designed to iterate on data sheet "
							+ this.dataSheetName
							+ " but that data sheet is already iterating as part of an enclosing loop action.");
		}
		/*
		 * are we to copy columns as fields?
		 */
		Value[] savedValues = null;
		if (this.columnsToCopyAsFields != null) {
			savedValues = this.saveFields(ctx, ds);
		}
		Value result = Value.VALUE_TRUE;
		int idx = 0;
		boolean interrupted = false;
		while (iterator.moveToNextRow()) {
			if (this.breakOnInterrupt && Thread.interrupted()) {
				interrupted = true;
				break;
			}
			if (this.columnsToCopyAsFields != null) {
				this.copyToFields(ctx, ds, idx);
			}

			JumpSignal signal = actionBlock.execute(driver);
			if (this.fieldsToCopyBackAsColumns != null) {
				this.copyToColumns(ctx, ds, idx);
			}
			if (signal == JumpSignal.STOP) {
				iterator.cancelIteration();
				result = Value.VALUE_FALSE;
				break;
			}
			if (signal == JumpSignal.BREAK) {
				iterator.cancelIteration();
				result = Value.VALUE_FALSE;
				break;
			}
			idx++;
		}
		if (savedValues != null) {
			this.restoreFields(ctx, ds, savedValues);
		}
		if (interrupted) {

			actionLogger.info("Coming out of loop because the thread is interrupted");

			Thread.currentThread().interrupt();
		}
		return result;
	}

	/**
	 * @param ctx
	 */
	private void copyToColumns(ServiceContext ctx, DataSheet ds, int idx) {
		if (this.copyBackAllColumns) {
			/*
			 * slightly optimized over getting individual columns..
			 */
			Value[] values = ds.getRow(idx);
			int i = 0;
			for (String fieldName : ds.getColumnNames()) {
				values[i++] = ctx.getValue(fieldName);
			}
			return;
		}
		for (String fieldName : this.fieldsToCopyBackAsColumns) {
			ds.setColumnValue(fieldName, idx, ctx.getValue(fieldName));
		}
	}

	/**
	 * @param ctx
	 */
	private void restoreFields(ServiceContext ctx, DataSheet ds, Value[] values) {
		int i = 0;
		if (this.copyAllColumnsToFields) {
			for (String fieldName : ds.getColumnNames()) {
				Value value = values[i++];
				if (value != null) {
					ctx.setValue(fieldName, value);
				}
			}
		} else {
			for (String fieldName : this.columnsToCopyAsFields) {
				Value value = values[i++];
				if (value != null) {
					ctx.setValue(fieldName, value);
				}
			}
		}
	}

	/**
	 * @param ctx
	 */
	private void copyToFields(ServiceContext ctx, DataSheet ds, int idx) {
		if (this.copyAllColumnsToFields) {
			/*
			 * slightly optimized over getting individual columns..
			 */
			Value[] values = ds.getRow(idx);
			int i = 0;
			for (String fieldName : ds.getColumnNames()) {
				ctx.setValue(fieldName, values[i++]);
			}
			return;
		}
		for (String fieldName : this.columnsToCopyAsFields) {
			ctx.setValue(fieldName, ds.getColumnValue(fieldName, idx));
		}
	}

	/**
	 * @param ctx
	 * @return
	 */
	private Value[] saveFields(ServiceContext ctx, DataSheet ds) {
		if (this.copyAllColumnsToFields) {
			Value[] values = new Value[ds.width()];
			int i = 0;
			for (String fieldName : ds.getColumnNames()) {
				values[i++] = ctx.getValue(fieldName);
			}
			return values;
		}
		Value[] values = new Value[this.columnsToCopyAsFields.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ctx.getValue(this.columnsToCopyAsFields[i]);
		}
		return values;
	}

	@Override
	public DbAccessType getDataAccessType() {
		return this.dbAccess;
	}

	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		/*
		 * loop action may want the caller to stop. We use this facility
		 */
		this.actionNameOnFailure = "_stop";

		if (this.columnsToCopyAsFields != null) {
			if (this.columnsToCopyAsFields.length == 1 && this.columnsToCopyAsFields[0].equals("*")) {
				this.copyAllColumnsToFields = true;
			}
		}

		if (this.fieldsToCopyBackAsColumns != null) {
			if (this.fieldsToCopyBackAsColumns.length == 1
					&& this.fieldsToCopyBackAsColumns[0].equals("*")) {
				this.copyBackAllColumns = true;
			}
		}
	}

	@Override
	public void validateSpecific(ValidationContext vtx, Service service) {
		super.validateSpecific(vtx, service);
		if (this.dataSheetName == null) {
			if (this.executeOnCondition == null && this.canJumpOut() == false) {
				/*
				 * appears to be an infinite loop
				 */
				if (service.executeInBackground) {
					/*
					 * it is okay to have infinite loops in background jobs, but
					 * we certainly encourage cancelable loops
					 */
					if (this.breakOnInterrupt == false) {
						vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_WARNING,
								"An infinite loop is used in the service, but breakOnInterrupt is not set. This may result in uncancellable jobs which are not desirable.",
								"breakOnInterrupt"));
					}
				} else {
					vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
							"Loop action should either have executeOnCondition or datasheet name", "datSheetName"));
				}
			}
		} else if (this.executeOnCondition != null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_INFO,
					"Loop action is for each row of a sheet, but it also specifies executeOnCondition. Note that the executeOnCondition is checked only once in the beginning to decide whether to start the loop at all. It is not checked for further itertions per row. Change your design if this is not the intended behaviour",
					"executeOnCondition"));
		}
		if (this.executeIfNoRowsInSheet != null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"executeIfNoRowsInSheet is invalid for loopAction.", "executeIfNoRowsInSheet"));
		}
		if (this.executeIfRowsInSheet != null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"executeIfRowsInSheet is invalid for loopAction.", "executeIfRowsInSheet"));
		}
	}

	/** @return true if this block has jumpAction with break/stop facility */
	private boolean canJumpOut() {
		if (this.actions == null) {
			return false;
		}
		for (Action action : this.actions) {
			if (action instanceof JumpTo) {
				if (((JumpTo) action).canJumpOut()) {
					return true;
				}
			}
		}
		return false;
	}
}
