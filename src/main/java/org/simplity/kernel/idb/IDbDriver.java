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

import org.simplity.kernel.db.DbVendor;

/**
 * @author simplity.org
 *
 */
public interface IDbDriver {
	/**
	 *
	 * @param dbUpdater
	 *            non-null. Object instance that uses ITransactionHandle to do
	 *            its transaction operations.
	 * @param schemName
	 *            null to work with default schema. non-default schema must be
	 *            one of the schema for which set-up details are specified in
	 *            the application configuration file.
	 */
	public void doTransaction(IDbUpdater dbUpdater, String schemName);

	/**
	 *
	 * @param reader
	 *            non-null object instance that does read operations on the db
	 *            using a read-only handle supplied to it
	 * @param schemName
	 *            null to work with default schema. non-default schema must be
	 *            one of the schema for which set-up details are specified in
	 *            the application configuration file.
	 */
	public void doRead(IDbReader reader, String schemName);

	/**
	 *
	 * @param dbUpdater
	 *            non-null object instance that uses ITransactionHandle to do
	 *            its own transaction processing (its own commit/rollback)
	 * @param schemName
	 *            null to work with default schema. non-default schema must be
	 *            one of the schema for which set-up details are specified in
	 *            the application configuration file.
	 */
	public void doMultipleTransactions(IDbUpdaterWithMutliTrans dbUpdater, String schemName);

	/**
	 *
	 * @param dbUpdater
	 *            a function that uses IAutoCommitHandle to do its db reads and
	 *            writes. all writes are auto-committed.
	 * @param schemName
	 *            null to work with default schema. non-default schema must be
	 *            one of the schema for which set-up details are specified in
	 *            the application configuration file.
	 */
	public void doAutocomitOperations(IDbUpdaterWithAutoCommit dbUpdater, String schemName);

	/**
	 * put % and escape the text suitable for a LIKE operation as per brand of
	 * RDBMS. we have standardized on ! as escape character
	 *
	 * @param text
	 *            to be escaped
	 * @return go ahead and send this as value of prepared statement for LIKE
	 */
	public String escapeForLike(String text);

	/**
	 * @param schema
	 *            name of schema to check for
	 * @return true is this schema is defined as additional schema. False
	 *         otherwise
	 */
	public boolean isSchemaDefined(String schema);

	/**
	 * @return default schema or null
	 */
	public String getDefaultSchema();

	/**
	 * @return dbVendor of the default driver
	 */
	public DbVendor getDbVendor();

	/**
	 * @return function, may be specific to the dbVendor, that is to be used to
	 *         get time-stamp value
	 */
	public String getTimeStamp();

	/**
	 * strcut parameters in sp are built based on data objects. Oracle does not
	 * conform to the standards, and hence this abstraction
	 *
	 * @return sql struct parameter, based on input data
	 */
	public StructCreator getStructCreator();

	/**
	 * array parameters in sp are built based on data objects. Oracle does not
	 * conform to the standards, and hence this abstraction
	 *
	 * @return sql array parameter that is constructed using data
	 */
	public ArrayCreator getArrayCreator();
}
