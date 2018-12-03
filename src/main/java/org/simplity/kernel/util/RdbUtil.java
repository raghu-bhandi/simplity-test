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

package org.simplity.kernel.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.MultiRowsSheet;
import org.simplity.kernel.idb.IResultSetReader;
import org.simplity.kernel.idb.IRowMetaData;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * utility related to JDBC related operations
 *
 * @author simplity.org
 *
 */
public class RdbUtil {
	private static final Logger logger = LoggerFactory.getLogger(RdbUtil.class);

	/*
	 * we store sql types with corresponding value types
	 */
	private static final int[] LONG_TYPES = { Types.BIGINT, Types.INTEGER, Types.SMALLINT };
	private static final int[] DOUBLE_TYPES = { Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.REAL };
	private static final int[] BOOLEAN_TYPES = { Types.BIT, Types.BOOLEAN };
	private static final Map<Integer, ValueType> SQL_TYPES = new HashMap<Integer, ValueType>();
	static {
		for (int i : LONG_TYPES) {
			SQL_TYPES.put(new Integer(i), ValueType.INTEGER);
		}
		SQL_TYPES.put(Types.DATE, ValueType.DATE);
		SQL_TYPES.put(Types.TIME, ValueType.DATE);
		SQL_TYPES.put(Types.TIMESTAMP, ValueType.TIMESTAMP);
		for (int i : DOUBLE_TYPES) {
			SQL_TYPES.put(new Integer(i), ValueType.DECIMAL);
		}
		for (int i : BOOLEAN_TYPES) {
			SQL_TYPES.put(new Integer(i), ValueType.BOOLEAN);
		}
	}

	/**
	 * log/trace a sql
	 *
	 * @param sql
	 * @param values
	 */
	public static void traceSql(String sql, Value[] values) {
		if (values == null || values.length == 0) {
			logger.info(sql);
			return;
		}
		StringBuilder sbf = new StringBuilder(sql);
		sbf.append("\n Parameters");
		int i = 0;
		for (Value value : values) {
			if (value == null) {
				break;
			}
			i++;
			sbf.append('\n').append(i).append(" : ").append(value.toString());
			if (i > 12) {
				sbf.append("..like wise up to ").append(values.length).append(" : ").append(values[values.length - 1]);
				break;
			}
		}
		logger.info(sbf.toString());
	}

	/**
	 * log/trace a sql
	 *
	 * @param sql
	 * @param values
	 */
	public static void traceBatchSql(String sql, Value[][] values) {
		logger.info("SQL :{}", sql);
		int i = 0;
		for (Value[] row : values) {
			if (row == null) {
				break;
			}
			i++;
			logger.info("SET {}", i);
			int j = 0;
			for (Value value : row) {
				if (value == null) {
					break;
				}
				j++;
				logger.info("{} :{} ", j, value);
			}
		}
	}

	private static class RsReader implements IResultSetReader {
		private final DataSheet sheet;

		protected RsReader(DataSheet sheet) {
			this.sheet = sheet;
		}

		@Override
		public void read(ResultSet rs) {
			try {
				ValueType[] types = this.sheet.getValueTypes();
				while (rs.next()) {
					this.sheet.addRow(resultToValueRow(rs, types));
				}
			} catch (SQLException e) {
				throw new ApplicationError(e, "");
			}
		}
	}

	/**
	 *
	 * @param sheet
	 *            to which data rows are to be added
	 * @return non-null. an instance that will append rows from rsult set into
	 *         the data sheet
	 */
	public static IResultSetReader sheetAppender(DataSheet sheet) {
		return new RsReader(sheet);
	}

	/**
	 *
	 * @param rs
	 *            non-null result set. must have data ready to be read/ been
	 *            true)
	 * @param types
	 *            non-null array of value types that correspond to the columns
	 *            in the result set
	 * @return non-null array of column values for the current row of thr result
	 *         set
	 * @throws SQLException
	 */
	public static Value[] resultToValueRow(ResultSet rs, ValueType[] types) throws SQLException {
		Value[] values = new Value[types.length];
		for (int i = 0; i < types.length; i++) {
			values[i] = types[i].extractFromRs(rs, i + 1);
		}
		return values;
	}

	/**
	 *
	 * @param rs
	 *            non-null result set. must have data ready to be read
	 * @param types
	 *            non-null array of value types that correspond to the columns
	 *            in the result set
	 * @param positions
	 *            non-null array. contains the 1-based position in the result
	 *            set
	 * @return row that has values, as per types
	 * @throws SQLException
	 */
	public static Value[] resultToValueRow(ResultSet rs, ValueType[] types, int[] positions) throws SQLException {
		Value[] values = new Value[types.length];
		for (int i = 0; i < types.length; i++) {
			values[i] = types[i].extractFromRs(rs, positions[i]);
		}
		return values;
	}

	/**
	 * get column names and types of a result set
	 *
	 * @param rs
	 * @return meta data
	 */
	public static IRowMetaData getColumnMetaData(ResultSet rs) {
		return new ColumnMetaData(rs);
	}

	/**
	 * get column names and types of a result set
	 *
	 * @param rs
	 * @return empty data sheet that has the right columns to receive data from
	 *         the result set
	 */
	public static DataSheet getDataSheetForSqlResult(ResultSet rs) {
		ColumnMetaData md = new ColumnMetaData(rs);
		return new MultiRowsSheet(md.getColumnNames(), md.getColumnValueTypes());
	}

	/**
	 * class that has the meta data about columns of a sql output
	 *
	 * @author simplity.org
	 *
	 */
	private static class ColumnMetaData implements IRowMetaData {

		private String[] names;
		private ValueType[] types;

		protected ColumnMetaData(ResultSet rs) {
			try {
				ResultSetMetaData md = rs.getMetaData();
				int n = md.getColumnCount();
				this.names = new String[n];
				this.types = new ValueType[n];
				for (int i = 0; i < n; i++) {
					this.names[i] = md.getColumnName(i + 1);
					this.types[i] = sqlTypeToValueType(md.getColumnType(i));
				}
			} catch (SQLException e) {
				throw new ApplicationError(e, "");
			}
		}

		@Override
		public String[] getColumnNames() {
			return this.names;
		}

		@Override
		public ValueType[] getColumnValueTypes() {
			return this.types;
		}
	}

	/**
	 * get valueType for sqltype
	 *
	 * @param sqlType
	 * @return value type
	 */
	public static ValueType sqlTypeToValueType(int sqlType) {
		ValueType type = SQL_TYPES.get(new Integer(sqlType));
		if (type == null) {
			return ValueType.TEXT;
		}
		return type;
	}
}
