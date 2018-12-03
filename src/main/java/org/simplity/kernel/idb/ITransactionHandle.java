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

package org.simplity.kernel.idb;

import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.db.ProcedureParameter;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * handle that implements a db-transaction. all db operations arried out using
 * this handle are under a transaction boundary. they are all either committed
 * or rolled-back at the end
 *
 * @author simplity.org
 *
 */
public interface ITransactionHandle extends IReadOnlyHandle {
	/**
	 * execute a sql to write/update data
	 *
	 * @param sql
	 *            prepared statement to be used to extract data from database
	 * @param values
	 *            array of values to be used to prepare the prepared statement
	 * @param treatSqlErrorAsNoAction
	 *            true if the sql is known to fail on no rows, and that is not
	 *            an error as per your design. for example, one of the common
	 *            designs is to try to modify a row, failing which the row is
	 *            inserted
	 * @return number of affected rows. 0 if no rows are ffected, -1 if this
	 *         data is not possible to get
	 *
	 */
	public int execute(String sql, Value[] values, boolean treatSqlErrorAsNoAction);

	/**
	 * execute a sql to write/update data using a prepared statement, but for
	 * more than one sets of data
	 *
	 * @param sql
	 *            prepared statement to be used to extract data from database
	 * @param values
	 *            list with each row with values to be used to prepare the
	 *            prepared statement
	 * @param treatSqlErrorAsNoAction
	 *            true if the sql is known to fail on no rows, and that is not
	 *            an error as per your design. for example, one of the common
	 *            designs is to try to modify a row, failing which the row is
	 *            inserted
	 * @return number of affected for each set of data. 0 if no rows are
	 *         affected, -1 if this data is not possible to get
	 *
	 */
	public int[] executeBatch(String sql, Value[][] values, boolean treatSqlErrorAsNoAction);

	/**
	 * execute an insert statement as a prepared statement
	 *
	 * @param sql
	 *            to be executed
	 * @param values
	 *            in the right order for the prepared statement
	 * @param generatedKeys
	 *            array in which generated keys are returned
	 * @param keyNames
	 *            array of names of columns that have generated keys. This is
	 *            typically just one, primary key
	 * @param treatSqlErrorAsNoAction
	 *            if true, sql error is treated as if rows affected is zero.
	 *            This is helpful when constraints are added in the db, and we
	 *            would treat failure as validation issue.
	 * @return 1 if row got inserted,0 otherwise
	 */
	public int insertAndGetKeys(String sql, Value[] values, long[] generatedKeys, String[] keyNames,
			boolean treatSqlErrorAsNoAction);

	/**
	 * execute a stored procedure that may update teh data base
	 *
	 * @param sql
	 *            must be in the standard jdbc format {call
	 *            procedureName(?,?,...)}
	 * @param inputFields
	 *            collection from which input fields are to be populated
	 * @param outputFields
	 *            collection to which output fields are to be extracted to
	 * @param params
	 *            procedure parameters, in the same order in which they are
	 *            defined in the db
	 * @param outputSheets
	 *            in case the stored procedure is designed to output multiple
	 *            result sets, data sheets are to be provided to extract data.
	 *            This feature is provided only to work with existing
	 *            applications. Such stored procedures are highly discouraged
	 * @param ctx
	 * @return number of rows extracted
	 */
	public int executeSp(String sql, FieldsCollection inputFields, FieldsCollection outputFields,
			ProcedureParameter[] params, DataSheet[] outputSheets, ServiceContext ctx);
}
