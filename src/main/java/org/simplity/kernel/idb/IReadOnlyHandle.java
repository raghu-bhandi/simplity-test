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

import java.io.Closeable;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;

import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.db.ProcedureParameter;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.service.ServiceContext;

/**
 * handle used to access data base only for reading with no locks.
 *
 * @author simplity.org
 *
 */
public interface IReadOnlyHandle extends Closeable {
	/**
	 * Handle is generally returned as part of begin-transaction kind of call,
	 * and will be active till the transaction is closed (commit/roll-back)
	 *
	 * @return true if this is active. false if it is closed, and should not be
	 *         used.
	 */
	public boolean isActive();

	/**
	 * read data from db using a one prepared statement and several sets of data
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            non-null. each row has an array of values to be used to
	 *            prepare the prepared statement
	 * @param reader
	 *            non-null. object instance that reads data from result set row.
	 */
	public void readBatch(String sql, Value[][] values, IResultSetReader reader);

	/**
	 * read data from db using a prepared statement
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            can be null if the prepared statement does not expect any
	 *            values. to be used for the parameters in the prepared
	 *            statement
	 * @param reader
	 *            non-null. object instance that reads data from result set row.
	 */
	public void read(String sql, Value[] values, IResultSetReader reader);

	/**
	 * read data from db using a one prepared statement and several sets of data
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            non-null. each row has an array of values to be used to
	 *            prepare the prepared statement
	 * @param outputTypes
	 *            non-null array of value types corresponding to the parameters
	 *            in the result-set row
	 * @param consumer
	 *            non-null. object instance that consumes data row.
	 */
	public void readBatch(String sql, Value[][] values, ValueType[] outputTypes, IDataRowConsumer consumer);

	/**
	 * read data from db using a prepared statement
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            can be null if the prepared statement does not expect any
	 *            values. to be used for the parameters in the prepared
	 *            statement
	 * @param outputTypes
	 *            non-null array of value types corresponding to the parameters
	 *            in the result-set row
	 * @param consumer
	 *            non-null. object instance that consumes data row.
	 */
	public void read(String sql, Value[] values, ValueType[] outputTypes, IDataRowConsumer consumer);

	/**
	 * read data from db using a one prepared statement and several sets of data
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            non-null. each row has an array of values to be used to
	 *            prepare the prepared statement
	 * @param dataSheet
	 *            non-null data sheet that has the right set of columns to
	 *            receive data from the result set
	 */
	public void readBatch(String sql, Value[][] values, DataSheet dataSheet);

	/**
	 * read data from db using a prepared statement
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            can be null if the prepared statement does not expect any
	 *            values. to be used for the parameters in the prepared
	 *            statement
	 * @param dataSheet
	 *            non-null data sheet that has the right set of columns to
	 *            receive data from the result set
	 */
	public void read(String sql, Value[] values, DataSheet dataSheet);

	/**
	 * Create a struct object for a stored procedure parameter
	 *
	 * @param values
	 *            array that has the value for each field/attribute in the
	 *            struct, in the right order
	 * @param dbObjectType
	 *            as defined in RDBMS
	 * @return object that can be assigned to a struct parameter
	 */
	public Struct createStruct(Value[] values, String dbObjectType);

	/**
	 * Create a struct object for a stored procedure parameter
	 *
	 * @param data
	 *            array that has the value for each field/attribute in the
	 *            struct, in the right order
	 * @param dbObjectType
	 *            as defined in RDBMS
	 * @return object that can be assigned to a struct parameter.
	 * @throws SQLException
	 */
	public Struct createStruct(Object[] data, String dbObjectType) throws SQLException;

	/**
	 * create an Array suitable as value for a stored procedure parameter
	 *
	 * @param values
	 *            array with values for creating Sql-Array
	 * @param dbArrayType
	 *            as defined in the RDBMS
	 * @return object that is suitable to be assigned to an array parameter
	 * @throws SQLException
	 */
	public Array createArray(Value[] values, String dbArrayType) throws SQLException;

	/**
	 * Create a struct array that can be assigned to procedure parameter. This
	 * is delegated to DBDriver because of issues with Oracle driver
	 *
	 * @param structs
	 *            array of structs from which to create the Array object for a
	 *            stored procedure
	 * @param dbArrayType
	 *            as defined in the rdbms
	 * @return object that is suitable to be assigned to stored procedure
	 *         parameter
	 * @throws SQLException
	 */
	public Array createStructArray(Struct[] structs, String dbArrayType) throws SQLException;

	/**
	 * caller is interested in knowing whether the sql fetches any data at all.
	 * Not interested in the actual data
	 *
	 * @param sql
	 * @param values
	 * @return true if result set has at least one row. false otherwise.
	 */
	public boolean hasData(String sql, Value[] values);

	/**
	 * read data from db using a prepared statement into a data sheet. This is
	 * to be used ONLY IF the output of SQL is NOT KNOWN at design time. This
	 * method has the over head of getting meta data about the result set. Other
	 * methods are preferred over this for that reason.
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            non-null. each row has an array of values to be used to
	 *            prepare the prepared statement
	 * @return dataSheet non-null. data sheet that has the data extracted from
	 *         db. empty if no data rows.
	 */
	public DataSheet readBatchIntoSheet(String sql, Value[][] values);

	/**
	 * read data from db using a prepared statement into a data sheet. This is
	 * to be used ONLY IF the output of SQL is NOT KNOWN at design time. This
	 * method has the over head of getting meta data about the result set. Other
	 * methods are preferred over this for that reason.
	 *
	 * @param sql
	 *            non-null. prepared statement to be used to extract data from
	 *            database
	 * @param values
	 *            can be null if the prepared statement does not expect any
	 *            values. to be used for the parameters in the prepared
	 *            statement
	 * @return dataSheet non-null. data sheet that has the data extracted from
	 *         db. empty if no data rows.
	 */
	public DataSheet readIntoSheet(String sql, Value[] values);

	/**
	 * extract output from stored procedure into data sheet
	 *
	 * @param sql
	 *            must be in the standard jdbc format {call
	 *            procedureName(?,?,...)}. This stored procedure should not do
	 *            any data-base updates, because this handle is meant to do
	 *            read-only operation. Procedures that update data base should
	 *            be part of a transaction.
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
	public int readUsingStoredProcedure(String sql, FieldsCollection inputFields, FieldsCollection outputFields,
			ProcedureParameter[] params, DataSheet[] outputSheets, ServiceContext ctx);
}
