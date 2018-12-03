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

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.idb.IDataRowConsumer;
import org.simplity.kernel.idb.IReadOnlyHandle;
import org.simplity.kernel.idb.IResultSetReader;
import org.simplity.kernel.util.RdbUtil;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author simplity.org
 *
 */
public class ReadonlyHandle implements IReadOnlyHandle {
	protected static final Logger logger = LoggerFactory.getLogger(ReadonlyHandle.class);
	private static final String ERROR = "SQLException while extracting data using prepared statement";
	/**
	 * connection object. null if this is closed.
	 */
	protected Connection connection;
	protected final RdbDriver driver;

	/**
	 * to be used by RdbDriver only.
	 *
	 * @param con
	 * @param traceSql
	 */
	ReadonlyHandle(Connection con, RdbDriver driver) {
		this.connection = con;
		this.driver = driver;
	}

	@Override
	public void close() {
		this.connection = null;
	}

	@Override
	public boolean isActive() {
		return this.connection != null;
	}

	@Override
	public void readBatch(String sql, Value[][] values, IResultSetReader reader) {
		this.checkActive();
		if (this.driver.logSqls) {
			RdbUtil.traceBatchSql(sql, values);
		}
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			for (Value[] vals : values) {
				setPreparedStatementParams(stmt, vals);
				ResultSet rs = stmt.executeQuery();
				reader.read(rs);
				rs.close();
			}
		} catch (SQLException e) {
			throw new ApplicationError(e, ERROR);
		}
	}

	@Override
	public void read(String sql, Value[] values, IResultSetReader reader) {
		this.checkActive();
		if (this.driver.logSqls) {
			RdbUtil.traceSql(sql, values);
		}
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			setPreparedStatementParams(stmt, values);
			ResultSet rs = stmt.executeQuery();
			reader.read(rs);
			rs.close();
		} catch (SQLException e) {
			throw new ApplicationError(e, ERROR);
		}
	}

	@Override
	public Struct createStruct(Value[] values, String dbObjectType) {
		this.checkActive();
		return null;
	}

	@Override
	public Struct createStruct(Object[] data, String dbObjectType) throws SQLException {
		this.checkActive();
		return this.driver.getStructCreator().createStruct(this.connection, data, dbObjectType);
	}

	@Override
	public Array createArray(Value[] values, String dbArrayType) throws SQLException {
		this.checkActive();
		Object[] data = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			Value val = values[i];
			if (val != null) {
				data[i] = val.toObject();
			}
		}

		return this.connection.createArrayOf(dbArrayType, data);
	}

	@Override
	public Array createStructArray(Struct[] structs, String dbArrayType) throws SQLException {
		this.checkActive();
		return this.driver.getArrayCreator().createArray(this.connection, structs, dbArrayType);
	}

	protected void checkActive() {
		if (this.connection == null) {
			throw new ApplicationError("Db operation attempted after the handle is closed");
		}
	}

	@Override
	public void readBatch(String sql, Value[][] values, ValueType[] outputTypes, IDataRowConsumer consumer) {
		this.readBatch(sql, values, new IResultSetReader() {

			@Override
			public void read(ResultSet rs) {
				try {
					while (rs.next()) {
						consumer.consume(RdbUtil.resultToValueRow(rs, outputTypes));
					}
				} catch (SQLException e) {
					throw new ApplicationError(e, "");
				}
			}
		});
	}

	@Override
	public void read(String sql, Value[] values, ValueType[] outputTypes, IDataRowConsumer consumer) {
		this.read(sql, values, new IResultSetReader() {

			@Override
			public void read(ResultSet rs) {
				try {
					while (rs.next()) {
						consumer.consume(RdbUtil.resultToValueRow(rs, outputTypes));
					}
				} catch (SQLException e) {
					throw new ApplicationError(e, "");
				}
			}
		});
	}

	@Override
	public void readBatch(String sql, Value[][] values, DataSheet dataSheet) {
		this.readBatch(sql, values, dataSheet.getValueTypes(), new IDataRowConsumer() {

			@Override
			public boolean consume(Value[] row) {
				dataSheet.addRow(row);
				return true;
			}
		});
	}

	@Override
	public void read(String sql, Value[] values, DataSheet dataSheet) {
		this.read(sql, values, dataSheet.getValueTypes(), new IDataRowConsumer() {

			@Override
			public boolean consume(Value[] row) {
				dataSheet.addRow(row);
				return true;
			}
		});
	}

	@Override
	public boolean hasData(String sql, Value[] values) {
		this.checkActive();
		if (this.driver.logSqls) {
			RdbUtil.traceSql(sql, values);
		}
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			setPreparedStatementParams(stmt, values);
			ResultSet rs = stmt.executeQuery();
			boolean result = rs.next();
			rs.close();
			return result;
		} catch (SQLException e) {
			throw new ApplicationError(e, ERROR);
		}
	}

	@Override
	public DataSheet readBatchIntoSheet(String sql, Value[][] values) {
		this.checkActive();
		if (this.driver.logSqls) {
			RdbUtil.traceBatchSql(sql, values);
		}
		DataSheet sheet = null;
		ValueType[] types = null;
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			for (Value[] vals : values) {
				setPreparedStatementParams(stmt, vals);
				ResultSet rs = stmt.executeQuery();
				if (sheet == null) {
					sheet = RdbUtil.getDataSheetForSqlResult(rs);
					types = sheet.getValueTypes();
				}
				while (rs.next()) {
					sheet.addRow(RdbUtil.resultToValueRow(rs, types));
				}
				rs.close();
			}
		} catch (SQLException e) {
			throw new ApplicationError(e, ERROR);
		}
		return sheet;
	}

	@Override
	public DataSheet readIntoSheet(String sql, Value[] values) {
		this.checkActive();
		if (this.driver.logSqls) {
			RdbUtil.traceSql(sql, values);
		}
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			setPreparedStatementParams(stmt, values);
			ResultSet rs = stmt.executeQuery();
			DataSheet sheet = RdbUtil.getDataSheetForSqlResult(rs);
			ValueType[] types = sheet.getValueTypes();
			while (rs.next()) {
				sheet.addRow(RdbUtil.resultToValueRow(rs, types));
			}
			rs.close();
			return sheet;
		} catch (SQLException e) {
			throw new ApplicationError(e, ERROR);
		}
	}

	/**
	 * set parameters to a prepared statement
	 *
	 * @param stmt
	 * @param values
	 * @throws SQLException
	 */
	protected static void setPreparedStatementParams(PreparedStatement stmt, Value[] values) throws SQLException {
		if (values == null) {
			return;
		}
		int i = 1;
		for (Value value : values) {
			if (value == null) {
				throw new ApplicationError(
						"Prepared statements should always get non-null values. use Value.UnknownxxxValue if needed");
			}
			value.setToStatement(stmt, i);
			i++;
		}
	}

	@Override
	public int readUsingStoredProcedure(String sql, FieldsCollection inputFields, FieldsCollection outputFields,
			ProcedureParameter[] params, DataSheet[] outputSheets, ServiceContext ctx) {
		int result = 0;
		SQLException err = null;
		try (CallableStatement stmt = this.connection.prepareCall(sql)) {
			if (params != null) {
				for (ProcedureParameter param : params) {
					/*
					 * programmers often make mistakes while defining
					 * parameters. Better to pin-point such errors
					 */
					try {
						if (param.setParameter(stmt, this, inputFields, ctx) == false) {
							logger.info("Error while setting " + param.name + " You will get an error.");
							/*
							 * issue in setting parameter. May be a mandatory
							 * field is not set
							 */
							return 0;
						}
					} catch (Exception e) {
						logger.info("Unable to set param " + param.name + " error : " + e.getMessage());
						param.reportError(e);
					}
				}
			}
			boolean hasResult = stmt.execute();
			int i = 0;
			if (outputSheets != null && hasResult) {
				int nbrSheets = outputSheets.length;
				while (hasResult) {
					if (i >= nbrSheets) {
						logger.info(
								"Stored procedure is ready to give more results, but the requester has supplied only "
										+ nbrSheets + " data sheets to read data into. Other data ignored.");
						break;
					}
					DataSheet outputSheet = outputSheets[i];
					ValueType[] outputTypes = outputSheet.getValueTypes();
					ResultSet rs = stmt.getResultSet();
					while (rs.next()) {
						outputSheet.addRow(RdbUtil.resultToValueRow(rs, outputTypes));
						result++;
					}
					rs.close();
					i++;
					hasResult = stmt.getMoreResults();
				}
			}
			if (params != null) {
				for (ProcedureParameter param : params) {
					try {
						param.extractOutput(stmt, outputFields, ctx);
					} catch (Exception e) {
						param.reportError(e);
					}
				}
			}
		} catch (SQLException e) {
			err = e;
		}
		if (err != null) {
			throw new ApplicationError(err, "Sql Error while extracting data using stored procedure");
		}
		logger.info(result + " rows extracted.");

		if (result > 0) {
			return result;
		}
		if (outputFields != null) {
			return 1;
		}
		return 0;
	}

}
