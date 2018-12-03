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

/**
 * db handle to get meta data about tables, columns schema etc..
 *
 * @author simplity.org
 *
 */
public interface IMetadataHandle {
	/**
	 * Handle is generally returned as part of begin-transaction kind of call,
	 * and will be active till the transaction is closed (commit/roll-back)
	 *
	 * @return true if this is active. false if it is closed, and should not be
	 *         used.
	 */
	public boolean isActive();

	/**
	 * get tables/views defined in the database
	 *
	 * @param schemaName
	 *            null, pattern or name
	 * @param tableName
	 *            null, pattern or name
	 * @return data sheet that has attributes for tables/views. Null if no
	 *         output
	 */
	public DataSheet getTables(String schemaName, String tableName);

	/**
	 * get column names of a table
	 *
	 * @param schemaName
	 *            schema to which this table belongs to. leave it null to get
	 *            the table from default schema
	 * @param tableName
	 *            can be null to get all tables or pattern, or actual name
	 * @return sheet with one row per column. Null if no columns.
	 */
	public DataSheet getTableColumns(String schemaName, String tableName);
}
