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
package org.simplity.kernel.db;

import java.sql.Connection;
import java.sql.Types;
import java.util.Map;

import javax.sql.DataSource;

import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We use DbDriver as a wrapper on JDBC to restrict the features to a smaller
 * subset that is easy to maintain.
 *
 * @author simplity.org
 */
public class DbDriver {
	private static final Logger logger = LoggerFactory.getLogger(DbDriver.class);

	// static final int[] TEXT_TYPES = {Types.CHAR, Types.LONGNVARCHAR,
	// Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.VARCHAR};

	/*
	 * for sql escaping
	 */
	private static final String OUR_ESCAPE_CHAR = "!";
	private static final String OUR_ESCAPE_STR = "!!";
	private static final String CONTEXT_PREFIX = "java:/comp/env/";

	/** character used in like operator to match any characters */
	public static final char LIKE_ANY = '%';
	/*
	 * meta 0-table columns, 1-primary keys, 2-procedure parameters. refer to
	 * meta.getColumnNames(), getPrimarykeys() and getProcedureColumns() of JDBC
	 */

	/*
	 * we are going to use value types s many time, it is ugly to use full name.
	 * Let us have some short and sweet names
	 */
	private static final ValueType INT = ValueType.INTEGER;
	private static final ValueType TXT = ValueType.TEXT;
	private static final ValueType BOOL = ValueType.BOOLEAN;
	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int TABLE_IDX = 0;
	private static final String[] TABLE_NAMES = { "schema", "tableName", "tableType", "remarks" };
	private static final ValueType[] TABLE_TYPES = { TXT, TXT, TXT, TXT };
	private static final int[] TABLE_POSNS = { 2, 3, 4, 5 };
	private static final String[] TABLE_TYPES_TO_EXTRACT = { "TABLE", "VIEW" };
	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int COL_IDX = 1;
	private static final String[] COL_NAMES = { "schema", "tableName", "columnName", "sqlType", "sqlTypeName", "size",
			"nbrDecimals", "remarks", "nullable" };
	private static final ValueType[] COL_TYPES = { TXT, TXT, TXT, INT, TXT, INT, INT, TXT, BOOL };
	private static final int[] COL_POSNS = { 2, 3, 4, 5, 6, 7, 9, 12, 18 };

	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int KEY_IDX = 2;
	private static final String[] KEY_NAMES = { "columnName", "sequence" };
	private static final ValueType[] KEY_TYPES = { TXT, INT };
	private static final int[] KEY_POSNS = { 4, 5 };

	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int PROC_IDX = 3;
	private static final String[] PROC_NAMES = { "schema", "procedureName", "procedureType", "remarks" };
	private static final ValueType[] PROC_TYPES = { TXT, TXT, INT, TXT };
	private static final int[] PROC_POSNS = { 2, 3, 8, 7 };

	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int PARAM_IDX = 4;
	private static final String[] PARAM_NAMES = { "schema", "procedureName", "paramName", "columnType", "sqlType",
			"sqlTypeName", "size", "precision", "scale", "remarks", "nullable", "position" };
	private static final ValueType[] PARAM_TYPES = { TXT, TXT, TXT, INT, INT, TXT, INT, INT, INT, TXT, BOOL, INT };
	private static final int[] PARAM_POSNS = { 2, 3, 4, 5, 6, 7, 9, 8, 10, 13, 19, 18 };

	/*
	 * names, types and positions as per result set for meta.getUDTs()
	 */
	private static final int STRUCT_IDX = 5;
	private static final String[] STRUCT_NAMES = { "schema", "structName", "structType", "remarks" };
	private static final ValueType[] STRUCT_TYPES = { TXT, TXT, TXT, TXT };
	private static final int[] STRUCT_POSNS = { 2, 3, 5, 6 };
	private static final int[] STRUCT_TYPES_TO_EXTRACT = { Types.STRUCT };
	/*
	 * names, types and positions as per result set for meta.getTables()
	 */
	private static final int ATTR_IDX = 6;
	private static final String[] ATTR_NAMES = { "schema", "structName", "attributeName", "sqlType", "sqlTypeName",
			"size", "nbrDecimals", "remarks", "nullable", "position" };
	private static final ValueType[] ATTR_TYPES = { TXT, TXT, TXT, INT, TXT, INT, INT, TXT, BOOL, INT };
	private static final int[] ATTR_POSNS = { 2, 3, 4, 5, 6, 7, 8, 11, 17, 16 };

	/*
	 * put them into array for modularity
	 */
	private static final String[][] META_COLUMNS = { TABLE_NAMES, COL_NAMES, KEY_NAMES, PROC_NAMES, PARAM_NAMES,
			STRUCT_NAMES, ATTR_NAMES };
	private static final ValueType[][] META_TYPES = { TABLE_TYPES, COL_TYPES, KEY_TYPES, PROC_TYPES, PARAM_TYPES,
			STRUCT_TYPES, ATTR_TYPES };
	private static final int[][] META_POSNS = { TABLE_POSNS, COL_POSNS, KEY_POSNS, PROC_POSNS, PARAM_POSNS,
			STRUCT_POSNS, ATTR_POSNS };

	// private static int numberOfKeysToGenerateAtATime = 100;

	/** are we to trace all sqls? Used during development/debugging */
	private static boolean traceSqls;
	/**
	 * We use either DataSource, or connection string to connect to the data
	 * base. DataSource is preferred
	 */
	private static DbVendor dbVendor;

	private static DataSource dataSource;
	private static String connectionString;

	/*
	 * this is set ONLY if the app is set for multi-schema. Stored at the time
	 * of setting the db driver
	 */
	private static String defaultSchema = null;

	private static Map<String, DataSource> otherDataSources = null;
	private static Map<String, String> otherConStrings = null;

	/*
	 * RDBMS brand dependent settings. set based on db vendor
	 */
	private static String timeStampFn;
	private static String[] charsToEscapeForLike;

	/** an open connection is maintained during execution of call back */
	private Connection connection;

	/**
	 * stated access type that is checked for consistency during subsequent
	 * calls
	 */
	private DbAccessType accessType;

	/** schema used by this connection */
	private String schemaName;
	/** just safety to ensure that our clients indeed follow the guidelines */
	private boolean forMultipleTrans;

	/**
	 * execute a prepared statement, with different sets of values
	 *
	 * @param sql
	 * @param values
	 *            each row should have the same number of values, in the right
	 *            order for the sql
	 * @param treatSqlErrorAsNoAction
	 *            if true, sql error is treated as if rows affected is zero.
	 *            This is helpful when constraints are added in the db, and we
	 *            would treat failure as validation issue.
	 * @return affected rows for each set of values
	 */
	public int[] executeBatch(String sql, Value[][] values, boolean treatSqlErrorAsNoAction) {
		if (traceSqls) {
			this.traceBatchSql(sql, values);
			if (this.connection == null) {
				return new int[0];
			}
		}
		this.checkWritable();
	}

}
