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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.UserTransaction;

import org.simplity.jms.JmsConnector;
import org.simplity.jms.JmsUsage;
import org.simplity.kernel.Application;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.Component;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.comp.ValidationUtil;
import org.simplity.kernel.data.DataPurpose;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.value.BooleanValue;
import org.simplity.kernel.value.Value;
import org.simplity.service.InputData;
import org.simplity.service.InputField;
import org.simplity.service.InputRecord;
import org.simplity.service.OutputData;
import org.simplity.service.ServiceContext;
import org.simplity.service.ServiceInterface;
import org.simplity.service.ServiceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction Processing Service
 *
 * @author simplity.org
 */
public class Service implements Component {

	static final Logger logger = LoggerFactory.getLogger(Service.class);

	/*
	 * constants used by on-the-fly services
	 */
	private static final char PREFIX_DELIMITER = '.';

	/** stop the execution of this service as success */
	public static final Value STOP_VALUE = Value.newTextValue("_s");
	/**
	 * field name with which result of an action is available in service context
	 */
	public static final String RESULT_SUFFIX = "Result";

	private static final ComponentType MY_TYPE = ComponentType.SERVICE;

	/** simple name */
	String name;

	/** module name.simpleName would be fully qualified name. */
	String moduleName;

	/**
	 * if this is implemented as a java code. If this is specified, no attribute
	 * (other than name and module name) are relevant
	 */
	@FieldMetaData(superClass = ServiceInterface.class, alternateField = "actions")
	String className;

	/** database access type */
	DbAccessType dbAccessType = DbAccessType.NONE;

	/**
	 * input fields/grids for this service. not valid if requestTextFieldName is
	 * specified
	 */
	InputData inputData;

	/** copy input records from another service */

	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.SERVICE)
	String referredServiceForInput;
	/** copy output records from another service */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.SERVICE)
	String referredServiceForOutput;

	/**
	 * schema name, different from the default schema, to be used specifically
	 * for this service
	 */
	String schemaName;
	/**
	 * output fields and grids for this service. Not valid if
	 * responseTextFieldName is specified
	 */
	OutputData outputData;

	/** actions that make up this service */
	Action[] actions;

	/** should this be executed in the background ALWAYS?. */
	boolean executeInBackground;

	/**
	 * can the response from this service be cached. If this is false, other
	 * caching related attributes are irrelevant.
	 */
	boolean okToCache;

	/**
	 * Should this response cache be discarded after certain time? 0 means no
	 * such predetermined validity
	 */
	@FieldMetaData(leaderField = "okToCache")
	int cacheValidityMinutes;

	/**
	 * valid only if okToCache is set to true. Use this if the service response
	 * can be cached for the same values for this list of fields. For example if
	 * getDistricts is a service that responds with all districts for the given
	 * state in the given countryCode then "countryCode,stateCode" is the value
	 * of this attribute.
	 *
	 * If the response is specific to given user, then _userId should be the
	 * first field in the list. Skip this if the response is not dependent on
	 * any input
	 */
	@FieldMetaData(leaderField = "okToCache")
	String[] cacheKeyNames;

	/**
	 * The cached response for services that are to be invalidated when this
	 * service is executed. for example updateStates service would invalidate
	 * cached responses from getStates service
	 */
	@FieldMetaData(irrelevantBasedOnField = "okToCache")
	String[] serviceCachesToInvalidate;

	/**
	 * does this service use jms? if so with what kind of transaction management
	 */
	JmsUsage jmsUsage;

	/** action names indexed to respond to navigation requests */
	private final HashMap<String, Integer> indexedActions = new HashMap<String, Integer>();

	/** flag to avoid repeated getReady() calls */
	private boolean gotReady;

	/** instance of className to be used as body of this service */
	private ServiceInterface serviceInstance;

	/**
	 * key names for services that are to be invalidated
	 */
	private String[][] invalidationKeys;

	/**
	 * if we want to offer cache-by-all-input-fields feature, this is the field
	 * that is populated at getReay()
	 */
	private String[] parsedCacheKeys;

	/**
	 * @return data base access required by this service
	 */
	public DbAccessType getDataAccessType() {
		return this.dbAccessType;
	}

	@Override
	public String getSimpleName() {
		return this.name;
	}

	/**
	 * should the service be fired in the background (in a separate thread)?
	 *
	 * @return true if this service is marked for background execution
	 */
	public boolean toBeRunInBackground() {
		return this.executeInBackground;
	}

	/**
	 * should the service be fired in the background (in a separate thread)?
	 *
	 * @return true if this service is marked for background execution
	 */
	public boolean okToCache() {
		return this.okToCache;
	}

	@Override
	public String getQualifiedName() {
		if (this.moduleName == null) {
			return this.name;
		}
		return this.moduleName + '.' + this.name;
	}

	/**
	 * execute this service as an independent service, but within a context that
	 * may be created by the main service DB has to be managed by this service.
	 * Also, we have an unusual return value type!!!
	 *
	 * @param ctx
	 *            service context
	 * @return application error if the service generated one. null if all ok
	 */
	private ApplicationError executeService(ServiceContext ctx, boolean transactionIsDelegeated) {
		/*
		 * resources that need to be released without fail..
		 */
		JmsConnector jmsConnector = null;
		UserTransaction userTransaciton = null;

		ApplicationError exception = null;
		BlockWorker worker = new BlockWorker(this.actions, this.indexedActions, ctx, this.serviceInstance,
				transactionIsDelegeated);
		/*
		 * execute all actions
		 */
		try {
			/*
			 * acquire resources that are needed for this service
			 */
			if (this.jmsUsage != null) {
				jmsConnector = JmsConnector.borrowConnector(this.jmsUsage);
				ctx.setJmsSession(jmsConnector.getSession());
			}

			DbAccessType access = this.dbAccessType;
			/*
			 * is this a JTA transaction?
			 */
			if (access == DbAccessType.EXTERNAL) {
				userTransaciton = Application.getUserTransaction();
				userTransaciton.begin();
			}
			if (access == DbAccessType.NONE) {
				worker.act(null);
			} else {
				/*
				 * Also, sub-Service means we open a read-only, and then call
				 * sub-service and complex-logic to manage their own connections
				 */
				if (access == DbAccessType.SUB_SERVICE) {
					access = DbAccessType.READ_ONLY;
				}
				DbDriver.workWithDriver(worker, access, this.schemaName);
			}
		} catch (ApplicationError e) {
			exception = e;
		} catch (Exception e) {
			exception = new ApplicationError(e, "Exception during execution of service. ");
		}
		/*
		 * close/return resources
		 */
		if (jmsConnector != null) {
			JmsConnector.returnConnector(jmsConnector, exception == null && ctx.isInError() == false);
		}
		if (userTransaciton != null) {
			try {
				if (exception == null && ctx.isInError() == false) {
					userTransaciton.commit();
				} else {

					logger.info("Service is in error. User transaction rolled-back");

					userTransaciton.rollback();
				}
			} catch (Exception e) {
				exception = new ApplicationError(e, "Error while commit/rollback of user transaction");
			}
		}
		return exception;
	}

	private boolean canWorkWithDriver(DbDriver driver) {
		/*
		 * use of JMS may trigger this irrespective of db access
		 */
		if (this.jmsUsage == JmsUsage.SERVICE_MANAGED || this.jmsUsage == JmsUsage.EXTERNALLY_MANAGED) {
			return false;
		}
		/*
		 * if we do not need it all, anything will do..
		 */
		if (this.dbAccessType == null || this.dbAccessType == DbAccessType.NONE) {
			return true;
		}
		/*
		 * can not work with null.
		 */
		if (driver == null) {
			return false;
		}

		/*
		 * may be we can get away for reads
		 */
		if (this.dbAccessType == DbAccessType.READ_ONLY) {
			if (this.schemaName == null || this.schemaName.equalsIgnoreCase(driver.getSchema())) {
				return true;
			}
		}

		/*
		 * we tried our best to re-use... but failed
		 */
		return false;
	}

	/**
	 * this service is called as a step in another service.
	 *
	 * @param ctx
	 * @param driver
	 * @param transactionIsDelegated
	 *            if true, supplied driver is for read-only. Parent service is
	 *            not managing transactions, and this service should manage its
	 *            transaction if needed
	 * @return return 0 if this service did not do intended work. positive
	 *         number if it did its work. This returned value is used as
	 *         workDone for the action.
	 */
	public BooleanValue executeAsAction(ServiceContext ctx, DbDriver driver, boolean transactionIsDelegated) {
		/*
		 * are we to manage our own transaction?
		 */
		if (transactionIsDelegated) {
			if (this.canWorkWithDriver(driver) == false) {
				/*
				 * execute this as a service
				 */
				ApplicationError err = this.executeService(ctx, transactionIsDelegated);
				if (err != null) {
					throw err;
				}
				return Value.VALUE_TRUE;
			}
		}

		BlockWorker worker = new BlockWorker(this.actions, this.indexedActions, ctx, this.serviceInstance,
				transactionIsDelegated);
		boolean result = worker.workWithDriver(driver);
		if (result) {
			return Value.VALUE_TRUE;
		}
		return Value.VALUE_FALSE;
	}

	@Override
	public void getReady() {
		if (this.gotReady) {

			logger.info("Service " + this.getQualifiedName()
					+ " is being harassed by repeatedly asking it to getReady(). Please look into this..");

			return;
		}
		this.gotReady = true;
		if (this.className != null) {
			try {
				this.serviceInstance = Application.getBean(this.className, ServiceInterface.class);
			} catch (Exception e) {
				throw new ApplicationError(e,
						"Unable to get an instance of service using class name " + this.className);
			}
		}
		if (this.actions == null) {

			logger.info("Service " + this.getQualifiedName() + " has no actions.");

			this.actions = new Action[0];
		} else {
			this.prepareChildren();
		}
		/*
		 * input record may have to be copied form referred service
		 */
		if (this.referredServiceForInput != null) {
			if (this.inputData != null) {
				throw new ApplicationError("Service " + this.getQualifiedName() + " refers to service "
						+ this.referredServiceForInput + " but also specifies its own input records.");
			}
			Service service = ComponentManager.getService(this.referredServiceForInput);
			this.inputData = service.inputData;
		}
		if (this.inputData != null) {
			this.inputData.getReady();
		}
		/*
		 * output record may have to be copied form referred service
		 */
		if (this.referredServiceForOutput != null) {
			if (this.outputData != null) {
				throw new ApplicationError("Service " + this.getQualifiedName() + " refers to service "
						+ this.referredServiceForOutput + " but also specifies its own output records.");
			}
			Service service = ComponentManager.getService(this.referredServiceForOutput);
			this.outputData = service.outputData;
		}
		if (this.outputData != null) {
			this.outputData.getReady();
		}

		if (this.serviceCachesToInvalidate != null) {
			this.invalidationKeys = new String[this.serviceCachesToInvalidate.length][];
			for (int i = 0; i < this.invalidationKeys.length; i++) {
				Service service = ComponentManager.getService(this.serviceCachesToInvalidate[i]);
				this.invalidationKeys[i] = service.getCacheKeyNames();
			}
		}

		if (this.okToCache) {
			this.setCacheKeys();
		}
	}

	private void prepareChildren() {
		int i = 0;
		boolean delegated = this.dbAccessType == DbAccessType.SUB_SERVICE;
		for (Action action : this.actions) {
			action.getReady(i, this);
			if (this.indexedActions.containsKey(action.actionName)) {
				throw new ApplicationError("Service " + this.name + " has duplicate action name " + action.actionName
						+ " as its action nbr " + (i + 1));
			}
			this.indexedActions.put(action.getName(), new Integer(i));
			i++;
			/*
			 * programmers routinely forget to set the dbaccess type.. we think
			 * it is worth this run-time over-head to validate it again
			 */
			if (delegated && (action instanceof ExecuteService)) {
				continue;
			}

			if (this.dbAccessType.canWorkWithChildType(action.getDataAccessType()) == false) {
				throw new ApplicationError(
						"Service " + this.getQualifiedName() + " uses dbAccessTYpe=" + this.dbAccessType
								+ " but action " + action.getName() + " requires " + action.getDataAccessType());
			}
		}
	}

	/**
	 * if caching keys are not set, we may infer it from input specification
	 */
	private void setCacheKeys() {
		if (this.cacheKeyNames != null) {
			this.parsedCacheKeys = this.cacheKeyNames;
			return;
		}

		InputField[] fields = null;
		if (this.inputData != null) {
			fields = this.inputData.getInputFields();
		}
		if (fields == null || fields.length == 0) {
			return;
		}

		this.parsedCacheKeys = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			this.parsedCacheKeys[i] = fields[i].getName();
		}
	}

	/*
	 * check for name and module name based on the requested name
	 */
	protected void setName(String possiblyQualifiedName) {
		int idx = possiblyQualifiedName.lastIndexOf('.');
		if (idx == -1) {
			this.name = possiblyQualifiedName;
			this.moduleName = null;
		} else {
			this.name = possiblyQualifiedName.substring(idx + 1);
			this.moduleName = possiblyQualifiedName.substring(0, idx);
		}
	}

	/**
	 * @param record
	 * @return
	 */
	static InputRecord[] getInputRecords(Record record) {
		String recordName = record.getQualifiedName();
		String[] children = record.getChildrenToInput();
		int nrecs = 1;
		if (children != null) {
			nrecs = children.length + 1;
		}

		InputRecord inRec = new InputRecord();
		inRec.setRecordName(recordName);
		inRec.setPurpose(DataPurpose.SAVE);
		inRec.enableSaveAction();

		InputRecord[] recs = new InputRecord[nrecs];
		recs[0] = inRec;
		if (children != null) {
			String sheetName = record.getDefaultSheetName();
			int i = 1;
			for (String child : children) {
				recs[i++] = ComponentManager.getRecord(child).getInputRecord(sheetName);
			}
		}
		return recs;
	}

	@Override
	public void validate(ValidationContext vtx) {
		ValidationUtil.validateMeta(vtx, this);
		if (this.actions != null) {
			this.validateChildren(vtx);
		}

		if (this.schemaName != null && DbDriver.isSchmeaDefined(this.schemaName) == false) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"schemaName is set to " + this.schemaName
							+ " but it is not defined as one of additional schema names in application.xml",
					"schemaName"));
		}
	}

	/**
	 * validate child actions
	 *
	 * @param vtx
	 */
	private void validateChildren(ValidationContext vtx) {
		Set<String> addedSoFar = new HashSet<String>();
		int actionNbr = 0;
		boolean dbAccessErrorRaised = false;
		boolean delegated = this.dbAccessType == DbAccessType.SUB_SERVICE;
		for (Action action : this.actions) {
			actionNbr++;
			if (action.actionName != null) {
				if (addedSoFar.add(action.actionName) == false) {
					vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
							"Duplicate action name " + action.actionName + " at " + actionNbr, null));
				}
			}
			action.validate(vtx, this);
			if (dbAccessErrorRaised || (delegated && (action instanceof ExecuteService))) {
				continue;
			}

			if (this.dbAccessType.canWorkWithChildType(action.getDataAccessType()) == false) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						"dbAccessType of service is not compatible for the dbAccessType of its actions. Please review your db access design thoroughly based on your actions design.",
						null));
				dbAccessErrorRaised = true;
			}
		}
	}

	@Override
	public ComponentType getComponentType() {
		return MY_TYPE;
	}

	/**
	 * generate a service on the fly, if possible
	 *
	 * @param serviceName
	 *            that follows on-the-fly-service-name pattern
	 * @return service, or null if name is not a valid on-the-fly service name
	 */
	public static Service generateService(String serviceName) {
		int idx = serviceName.lastIndexOf(PREFIX_DELIMITER);
		if (idx == -1) {
			logger.info("service {} is not meant to be generated on-the-fly");
			return null;
		}

		String recordName = serviceName.substring(0, idx);
		Record record = ComponentManager.getRecordOrNull(recordName);
		if (record == null) {
			logger.info("{} is not defined as a record, and hence we did not attempt generating service for {}.",
					recordName, serviceName);
			return null;
		}

		String operation = serviceName.substring(idx + 1);
		ServiceOperation oper = null;
		try {
			oper = ServiceOperation.valueOf(operation.toUpperCase());
		} catch (Exception e) {
			// we know that the only reason is that it is not a valid operation
		}
		if (oper == null) {
			logger.info("{} is not a valid operation, and hence we did not attempt generating a service for  {}",
					operation, serviceName);
			return null;
		}
		return oper.generateService(serviceName, record);
	}

	/**
	 * @return list of service-cache to be invalidated when this service is
	 *         executed. This is the list of cache that would be affected when
	 *         this service updates some data
	 */
	public String[] getServicesToInvalidate() {
		return this.serviceCachesToInvalidate;
	}

	/**
	 *
	 * @return time in minutes after which the cached response for this service
	 *         should be refreshed or invalidated. a value of 0 implies that the
	 *         service-cache validity is not time-bound.
	 */
	public int getCacheRefreshTime() {
		return this.cacheValidityMinutes;
	}

	/**
	 * what kind of data is expected as input for this service from the client.
	 * It is up to the caller (client-agent) to ensure that a valid data is made
	 * available in service context before calling this service. Service does
	 * not repeat the validations, and hence if the caller has not validated
	 * data,it will lead to run-time exceptions
	 *
	 * @return input data specification. null if this is a sub-service, or a
	 *         utility service that may accept anything and everything that has
	 *         come from client
	 */

	public InputData getInputSpecification() {
		return this.inputData;
	}

	/**
	 * what data need to be sent as response to the client. Service would have
	 * ensured that the data elements are available in the service context for
	 * the caller to pick them up and prepare a response
	 *
	 * @return output data specification, or null if everything from context is
	 *         to be sent back as response.
	 */
	public OutputData getOutputSpecification() {
		return this.outputData;
	}

	/**
	 * data input-output is managed by the caller. Just execute this service in
	 * this context
	 *
	 * @param ctx
	 */
	public void serve(ServiceContext ctx) {
		ApplicationError err = this.executeService(ctx, false);
		if (err != null) {
			throw err;
		}
		if (this.okToCache()) {
			String key = createCachingKey(this.getQualifiedName(), this.cacheKeyNames, ctx);
			ctx.setCaching(key, this.cacheValidityMinutes);
		} else if (this.serviceCachesToInvalidate != null) {
			ctx.setInvalidations(this.getInvalidations(ctx));
		}
	}

	private String[] getInvalidations(ServiceContext ctx) {
		String[] result = new String[this.serviceCachesToInvalidate.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = createCachingKey(this.serviceCachesToInvalidate[i], this.invalidationKeys[i], ctx);
		}
		return result;
	}

	/**
	 * separator character between key field values to form a single string of
	 * key
	 */
	public static final char CACHE_KEY_SEP = '\0';

	/**
	 * @param ctx
	 * @return
	 */
	private static String createCachingKey(String serviceName, String[] keyNames, ServiceContext ctx) {
		if (keyNames == null) {
			return createCachingKey(serviceName, null);
		}
		String[] vals = new String[keyNames.length];
		/*
		 * first field could be userId
		 */
		int startAt = 0;
		if (keyNames[0].equals(ServiceProtocol.USER_ID)) {
			startAt = 1;
			vals[0] = ctx.getUserId().toString();
		}
		for (int i = startAt; i < keyNames.length; i++) {
			Value val = ctx.getValue(keyNames[i]);
			if (val != null) {
				vals[i] = val.toString();
			}
		}

		return createCachingKey(serviceName, vals);
	}

	/**
	 * form a key to be used for caching based on service name and values of
	 * keys. This method to be used for caching and retrieving
	 *
	 * @param serviceName
	 * @param keyValues
	 * @return key to be used for caching
	 */
	public static String createCachingKey(String serviceName, String[] keyValues) {
		if (keyValues == null) {
			return serviceName;
		}
		StringBuilder result = new StringBuilder(serviceName);
		for (String val : keyValues) {
			result.append(CACHE_KEY_SEP);
			if (val != null) {
				result.append(val);
			}
		}

		return result.toString();
	}

	/**
	 *
	 * @return names of keys on which this service can be cached. Null either if
	 *         this service is never cached, or it is cched with no keys
	 */
	public String[] getCacheKeyNames() {
		return this.cacheKeyNames;
	}
}
