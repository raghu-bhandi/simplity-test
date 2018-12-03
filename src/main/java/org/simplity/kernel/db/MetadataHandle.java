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

package org.simplity.kernel.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.MultiRowsSheet;
import org.simplity.kernel.idb.IMetadataHandle;
import org.simplity.kernel.value.IntegerValue;
import org.simplity.kernel.value.Value;

/**
 * @author simplity.org
 *
 */
public class MetadataHandle implements IMetadataHandle {
	private Connection connection;
	private RdbDriver driver;

	MetadataHandle(Connection con, RdbDriver driver) {
		this.connection = con;
		this.driver = driver;
	}

	@Override
	public boolean isActive() {
		return false;
	}

	@Override
	public DataSheet getTables(String schemaName, String tableName) {
		return null;
	}

	@Override
	public DataSheet getTableColumns(String schemaName, String tableName) {
		return null;
	}

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
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public static DataSheet getTables(String schemaName, String tableName) {
		Connection con = getConnection();
		try {
			return getMetaSheet(con, schemaName, tableName, TABLE_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

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
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public static DataSheet getTableColumns(String schemaName, String tableName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schemaName);
		try {
			return getMetaSheet(con, schemaName, tableName, COL_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get key columns for all tables in the schema
	 *
	 * @param schemaName
	 * @return sheet with one row per column. Null if this table does not exist,
	 *         or something went wrong!!
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public static DataSheet getPrimaryKeys(String schemaName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schemaName);
		try {
			return getMetaSheet(con, schemaName, null, KEY_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get key columns names of a table
	 *
	 * @param schemaName
	 *            possibly null
	 * @param tableName
	 *            non-null
	 * @return key column names
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public static String[] getPrimaryKeysForTable(String schemaName, String tableName) {
		if (tableName == null) {

			logger.info(
					"getPrimaryKeysForTable() is for a specific table. If you want for all tables, use the getPrimaryKeys()");

			return null;
		}
		Connection con = getConnection(DbAccessType.READ_ONLY, schemaName);
		try {
			DataSheet sheet = getMetaSheet(con, schemaName, tableName, KEY_IDX);
			if (sheet == null) {
				return null;
			}
			int n = sheet.length();
			String[] result = new String[n];
			for (int i = 0; i < n; i++) {
				Value[] row = sheet.getRow(i);
				int idx = (int) ((IntegerValue) row[1]).getLong() - 1;
				result[idx] = row[0].toString();
			}
			return result;
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get stored procedures
	 *
	 * @param schemaName
	 *            null, pattern or name
	 * @param procedureName
	 *            null, pattern or name
	 * @return data sheet that has attributes of procedures. Null if no output
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public static DataSheet getProcedures(String schemaName, String procedureName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schemaName);
		try {
			return getMetaSheet(con, schemaName, procedureName, PROC_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get parameters of procedure
	 *
	 * @param schema
	 *            null, pattern or name
	 * @param procedureName
	 *            null, pattern or name
	 * @return sheet with one row per column. Null if this table does not exist,
	 *         or something went wrong!!
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public DataSheet getProcedureParams(String schema, String procedureName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schema);
		try {
			return getMetaSheet(con, schema, procedureName, PARAM_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get structures/user defined types
	 *
	 * @param schema
	 *            null, pattern, or name
	 * @param structName
	 *            null or pattern.
	 * @return data sheet containing attributes of structures. Null of no output
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public DataSheet getStructs(String schema, String structName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schema);
		try {
			return getMetaSheet(con, schema, structName, STRUCT_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	/**
	 * get attributes of structure (user defined data type)
	 *
	 * @param schema
	 *            null for all or pattern/name
	 * @param structName
	 *            null for all or pattern/name
	 * @return sheet with one row per column. Null if no output
	 */
	@SuppressWarnings("resource")
	// closeConnection() used instead
	public DataSheet getStructAttributes(String schema, String structName) {
		Connection con = getConnection(DbAccessType.READ_ONLY, schema);
		try {
			return getMetaSheet(con, schema, structName, ATTR_IDX);
		} finally {
			closeConnection(con, DbAccessType.READ_ONLY, true);
		}
	}

	private static DataSheet getMetaSheet(Connection con, String schema, String metaName, int metaIdx) {
		ResultSet rs = null;
		String schemaName = schema;
		if (schema == null && defaultSchema != null && defaultSchema.equals("PUBLIC") == false) {
			schemaName = defaultSchema;
		}
		try {
			DatabaseMetaData meta = con.getMetaData();
			switch (metaIdx) {
			case TABLE_IDX:
				logger.info("trying tables with schema={}, meta={}", schemaName, metaIdx);
				rs = meta.getTables(null, schemaName, metaName, TABLE_TYPES_TO_EXTRACT);
				break;
			case COL_IDX:
				rs = meta.getColumns(null, schemaName, metaName, null);
				break;
			case KEY_IDX:
				rs = meta.getPrimaryKeys(null, schemaName, metaName);
				break;
			case PROC_IDX:
				rs = meta.getProcedures(null, schemaName, metaName);
				break;
			case PARAM_IDX:
				rs = meta.getProcedureColumns(null, schemaName, metaName, null);
				break;
			case STRUCT_IDX:
				rs = meta.getUDTs(null, schemaName, metaName, STRUCT_TYPES_TO_EXTRACT);
				break;
			case ATTR_IDX:
				rs = meta.getAttributes(null, schemaName, metaName, null);
				break;
			default:
				throw new ApplicationError("Meta data " + metaIdx + " is not defined yet.");
			}
			if (rs.next()) {
				DataSheet sheet = new MultiRowsSheet(META_COLUMNS[metaIdx], META_TYPES[metaIdx]);
				do {
					sheet.addRow(getParams(rs, META_TYPES[metaIdx], META_POSNS[metaIdx]));
				} while (rs.next());
				return sheet;
			}
			logger.warn("Result set is empty for meta data");
		} catch (Exception e) {
			logger.error("Unable to get meta data for " + metaName, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
		}
		logger.warn("Returnig null data sheet for eta data");
		return null;
	}

}
