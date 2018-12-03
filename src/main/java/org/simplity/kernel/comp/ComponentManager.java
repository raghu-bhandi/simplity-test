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
package org.simplity.kernel.comp;

import org.simplity.adapter.DataAdapter;
import org.simplity.job.BatchJobs;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.Message;
import org.simplity.kernel.data.FieldsCollection;
import org.simplity.kernel.db.Sql;
import org.simplity.kernel.db.StoredProcedure;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.dt.DataType;
import org.simplity.kernel.fn.Function;
import org.simplity.kernel.value.Value;
import org.simplity.service.ExternalService;
import org.simplity.test.TestRun;
import org.simplity.tp.Service;

/**
 * Utility class that provides component specific methods to get instances. This
 * approach is an alternate to providing generic methods. This is more like a
 * wrapper class on <code>ComponentType</code>
 *
 * @author simplity.org
 */
public class ComponentManager {
	/**
	 * external services are indexed by appName + APP_SEP + serviceName
	 */
	public static final char APP_SEP = ':';

	/**
	 * @param messageName
	 *            name
	 * @return message if it is defined, null otherwise
	 */
	public static Message getMessageOrNull(String messageName) {
		return (Message) ComponentType.MSG.getComponentOrNull(messageName);
	}

	/**
	 * note that this throws an error if message is not found
	 *
	 * @param messageName
	 *            name
	 * @return message if it is defined, error otherwise
	 */
	public static Message getMessage(String messageName) {
		Component comp = ComponentType.MSG.getComponentOrNull(messageName);
		if (comp == null) {
			throw new ApplicationError(messageName + " is not a valid message.");
		}
		return (Message) comp;
	}

	/**
	 * @param dataTypeName
	 *            name
	 * @return dataType if it is defined, null otherwise
	 */
	public static DataType getDataTypeOrNull(String dataTypeName) {
		return (DataType) ComponentType.DT.getComponentOrNull(dataTypeName);
	}

	/**
	 * note that this throws an error if data type is not found
	 *
	 * @param dataTypeName
	 *            name
	 * @return DataType if it is defined, error otherwise
	 */
	public static DataType getDataType(String dataTypeName) {
		Component comp = ComponentType.DT.getComponentOrNull(dataTypeName);
		if (comp == null) {
			throw new ApplicationError(dataTypeName + " is not a valid data type");
		}
		return (DataType) comp;
	}

	/**
	 * @param recordName
	 *            name
	 * @return Record if it is defined, null otherwise
	 */
	public static Record getRecordOrNull(String recordName) {
		return (Record) ComponentType.REC.getComponentOrNull(recordName);
	}

	/**
	 * note that this throws an error if record is not found
	 *
	 * @param recordName
	 *            name
	 * @return Record if it is defined, error otherwise
	 */
	public static Record getRecord(String recordName) {
		Component comp = ComponentType.REC.getComponentOrNull(recordName);
		if (comp == null) {
			throw new ApplicationError(recordName + " is not a valid record.");
		}
		return (Record) comp;
	}

	/**
	 * @param serviceName
	 *            name
	 * @return Service if it is defined, null otherwise
	 */
	public static Service getServiceOrNull(String serviceName) {
		return (Service) ComponentType.SERVICE.getComponentOrNull(serviceName);
	}

	/**
	 * note that this throws an error if service is not found
	 *
	 * @param serviceName
	 *            name
	 * @return Service if it is defined, error otherwise
	 */
	public static Service getService(String serviceName) {
		Service service = getServiceOrNull(serviceName);
		if (service == null) {
			throw new ApplicationError(serviceName + " is not a valid service.");
		}
		return service;
	}

	/**
	 * @param JobsName
	 *            name
	 * @return Jobs if it is defined, null otherwise
	 */
	public static BatchJobs getJobsNull(String JobsName) {
		return (BatchJobs) ComponentType.JOBS.getComponentOrNull(JobsName);
	}

	/**
	 * note that this throws an error if Jobs is not found
	 *
	 * @param jobsName
	 *            name
	 * @return Jobs if it is defined, error otherwise
	 * @throws ApplicationError
	 */
	public static BatchJobs getJobs(String jobsName) {
		Component comp = ComponentType.JOBS.getComponentOrNull(jobsName);
		if (comp == null) {
			throw new ApplicationError(jobsName + " is not a valid Job.");
		}
		return (BatchJobs) comp;
	}

	/**
	 * @param sqlName
	 *            name
	 * @return Sql if it is defined, null otherwise
	 */
	public static Sql getSqlOrNull(String sqlName) {
		return (Sql) ComponentType.SQL.getComponentOrNull(sqlName);
	}

	/**
	 * note that this throws an error if sql is not found
	 *
	 * @param sqlName
	 *            name
	 * @return message if it is defined, error otherwise
	 */
	public static Sql getSql(String sqlName) {
		Component comp = ComponentType.SQL.getComponentOrNull(sqlName);
		if (comp == null) {
			throw new ApplicationError(sqlName + " is not a valid SQL.");
		}
		return (Sql) comp;
	}

	/**
	 * @param procedureName
	 *            name
	 * @return StoredProcedure if it is defined, null otherwise
	 */
	public static StoredProcedure getStoredProcedureOrNull(String procedureName) {
		return (StoredProcedure) ComponentType.SP.getComponentOrNull(procedureName);
	}

	/**
	 * note that this throws an error if stored procedure is not found
	 *
	 * @param procedureName
	 *            name
	 * @return StoredProcedure if it is defined, error otherwise
	 */
	public static StoredProcedure getStoredProcedure(String procedureName) {
		Component comp = ComponentType.SP.getComponentOrNull(procedureName);
		if (comp == null) {
			throw new ApplicationError(procedureName + " is not a valid stored procedure.");
		}
		return (StoredProcedure) comp;
	}

	/**
	 * @param functionName
	 *            name
	 * @return a function if it is defined, null otherwise
	 */
	public static Function getFunctionOrNull(String functionName) {
		return (Function) ComponentType.FUNCTION.getComponentOrNull(functionName);
	}

	/**
	 * note that this throws an error if function is not found
	 *
	 * @param functionName
	 *            name
	 * @return StoredProcedure if it is defined, error otherwise
	 */
	public static Function getFunction(String functionName) {
		Component comp = ComponentType.FUNCTION.getComponentOrNull(functionName);
		if (comp == null) {
			throw new ApplicationError(functionName + " is not a valid message.");
		}
		return (Function) comp;
	}

	/**
	 * evaluate a function and return its value
	 *
	 * @param functionName
	 *            name of function
	 * @param valueList
	 *            array of arguments. Must match arguments of function in the
	 *            right order. null
	 *            or empty array if this function does not require any arguments
	 * @param data
	 *            fields context that may contain other fields that the function
	 *            may refer at run
	 *            time. This is typically the fields from serviceCOntext
	 * @return value is never null. However value.isNull() could be true.
	 * @throws ApplicationError
	 *             in case the function is not defined, or you are passing wrong
	 *             type of
	 *             arguments for the function
	 */
	public static Value evaluate(String functionName, Value[] valueList, FieldsCollection data) {
		return getFunction(functionName).execute(valueList, data);
	}

	/**
	 * get a test case
	 *
	 * @param caseName
	 * @return test case. not null
	 * @throws ApplicationError
	 *             in case the testCase is not found.
	 */
	public static TestRun getTestRun(String caseName) {
		Component comp = ComponentType.TEST_RUN.getComponentOrNull(caseName);
		if (comp == null) {
			throw new ApplicationError(caseName + " is not a valid test case.");
		}
		return (TestRun) comp;
	}

	/**
	 * get a test case
	 *
	 * @param caseName
	 * @return test case, or null.
	 */
	public static TestRun getTestRunOrNull(String caseName) {
		return (TestRun) ComponentType.TEST_RUN.getComponentOrNull(caseName);
	}

	/** @return component folder */
	public static String getComponentFolder() {
		return ComponentType.getComponentRoot();
	}

	/**
	 *
	 * @param appName
	 *            applicationName
	 * @param serviceName
	 *            serviceName
	 * @return component, or null in case it is missing in action
	 */
	public static ExternalService getExternalServiceOrNull(String appName, String serviceName) {
		String key = toExternalKey(appName, serviceName);
		return (ExternalService) ComponentType.EXTERN.getComponentOrNull(key);
	}

	/**
	 * @param appName
	 * @param serviceName
	 * @return
	 */
	private static String toExternalKey(String appName, String serviceName) {
		return appName + APP_SEP + serviceName;
	}

	/**
	 *
	 * @param appName
	 *            applicationName
	 * @param serviceName
	 *            serviceName
	 * @return non-null component. ApplicationError is raised in case the
	 *         component is missing
	 */
	public static ExternalService getExternalService(String appName, String serviceName) {
		ExternalService extern = getExternalServiceOrNull(appName, serviceName);
		if (extern == null) {
			throw new ApplicationError("No external service named " + serviceName + " for application " + appName);
		}
		return extern;
	}

	/**
	 * let components be cached once they are loaded. Typically used in
	 * production environment
	 */
	public static void startCaching() {
		ComponentType.startCaching();
	}

	/**
	 * purge cached components, and do not cache any more. USed during development.
	 */
	public static void stopCaching() {
		ComponentType.stopCaching();
	}

	/**
	 * must be called before asking for any component. May also be called to
	 * reset at any time.
	 *
	 * @param rootPath
	 * @param modules
	 */
	public static void bootstrap(String rootPath, String[] modules) {
		ComponentType.bootstrap(rootPath, modules);
	}

	/**
	 * @return return the root path for components. This is the root path under
	 *         which all components can be located.
	 */
	public static String getComponentRoot() {
		return ComponentType.getComponentRoot();
	}

	/**
	 * @param adapterName
	 * @return adapter. never null. ApplicationError thrown in case this adapter is not found
	 */
	public static DataAdapter getAdapterOrNull(String adapterName) {
		return (DataAdapter) ComponentType.ADAPTER.getComponentOrNull(adapterName);
	}
	/**
	 * @param adapterName
	 * @return adapter. never null. ApplicationError thrown in case this adapter is not found
	 */
	public static DataAdapter getAdapter(String adapterName) {
		DataAdapter adapter =  (DataAdapter) ComponentType.ADAPTER.getComponentOrNull(adapterName);
		if (adapter == null) {
			throw new ApplicationError("No data adapter named " + adapterName);
		}
		return adapter;
	}
}
