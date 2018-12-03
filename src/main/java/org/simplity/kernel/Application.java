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
package org.simplity.kernel;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

import org.simplity.adapter.DataAdapterExtension;
import org.simplity.auth.OAuthParameters;
import org.simplity.gateway.Gateway;
import org.simplity.gateway.Gateways;
import org.simplity.jms.JmsConnector;
import org.simplity.job.BatchJobs;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.comp.ValidationUtil;
import org.simplity.kernel.db.RdbDriver;
import org.simplity.kernel.dm.CommonCodeValidator;
import org.simplity.kernel.dm.CommonCodeValidatorInterface;
import org.simplity.kernel.dm.ParameterRetriever;
import org.simplity.kernel.dm.ParameterRetrieverInterface;
import org.simplity.kernel.file.FileBasedAssistant;
import org.simplity.kernel.mail.MailConnector;
import org.simplity.kernel.mail.MailProperties;
import org.simplity.kernel.util.IoUtil;
import org.simplity.kernel.util.XmlUtil;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.sa.AppUser;
import org.simplity.sa.ServiceAgent;
import org.simplity.sa.ServicePrePostProcessorInterface;
import org.simplity.sa.ServiceRequest;
import org.simplity.service.AccessControllerInterface;
import org.simplity.service.ServiceCacherInterface;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configure this application
 *
 * @author simplity.org
 */
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	private static final String FOLDER_STR = "/";
	/**
	 * we use a default user id during testing
	 */
	private static final Value DEFAULT_NUMERIC_USER_ID = Value.newIntegerValue(420);
	private static final Value DEFAULT_TEXT_USER_ID = Value.newTextValue("420");

	/**
	 * any exception thrown by service may need to be reported to a central
	 * system.
	 */
	private static ExceptionListenerInterface currentExceptionListener = new DefaultExceptionListener();

	private static AppDataCacherInterface currentAppDataCacher = null;
	/**
	 * instance of a UserTransaction for JTA/JCA based transaction management
	 */
	private static Object userTransactionInstance;

	private static boolean userIdIsNumeric;

	/*
	 * batch and thread management
	 */
	private static ThreadFactory threadFactory;
	private static ScheduledExecutorService threadPoolExecutor;
	private static int batchPoolSize;

	/**
	 *
	 * @return app data cacher that is set up for this app. null if no acher is
	 *         configured
	 */
	public static AppDataCacherInterface getAppDataCacher() {
		return currentAppDataCacher;
	}

	/**
	 * report an application error that needs attention from admin
	 *
	 * @param e
	 */
	public static void reportApplicationError(ApplicationError e) {
		currentExceptionListener.listen(e);
	}

	/**
	 * report an application error that needs attention from admin
	 *
	 * @param e
	 */
	public static void reportApplicationError(Exception e) {
		currentExceptionListener.listen(e);
	}

	/**
	 * report an exception that needs attention from admin
	 *
	 * @param request
	 *            data with which service was invoked. null if the error has no
	 *            such reference
	 * @param e
	 */
	public static void reportApplicationError(ServiceRequest request, Exception e) {
		currentExceptionListener.listen(request, new ApplicationError(e, ""));
	}

	/** @return get a UserTrnsaction instance */
	public static UserTransaction getUserTransaction() {
		if (userTransactionInstance == null) {
			throw new ApplicationError("Application is not set up for a JTA based user transaction");
		}
		return (UserTransaction) userTransactionInstance;
	}

	/**
	 * @return default user id, typically during tests. null if it is not set
	 */
	public static Value getDefaultUserId() {
		if (userIdIsNumeric) {
			return DEFAULT_NUMERIC_USER_ID;
		}
		return DEFAULT_TEXT_USER_ID;
	}

	/** @return is the userId a number? default is text/string */
	public static boolean userIdIsNumeric() {
		return userIdIsNumeric;
	}

	/**
	 * get a managed thread as per the container
	 *
	 * @param runnable
	 * @return thread
	 */
	public static Thread createThread(Runnable runnable) {
		if (threadFactory == null) {
			return new Thread(runnable);
		}
		return threadFactory.newThread(runnable);
	}

	/**
	 * get a managed thread as per the container
	 *
	 * @return executor
	 */
	public static ScheduledExecutorService getScheduledExecutor() {
		if (threadPoolExecutor != null) {
			return threadPoolExecutor;
		}
		int nbr = batchPoolSize;
		if (nbr == 0) {
			nbr = 2;
		}
		if (threadFactory == null) {
			return new ScheduledThreadPoolExecutor(nbr);
		}
		threadPoolExecutor = new ScheduledThreadPoolExecutor(nbr, threadFactory);
		return threadPoolExecutor;
	}

	/** name of configuration file, including extension */
	public static final String CONFIG_FILE_NAME = "application.xml";

	/**
	 * One and the only method that is to be executed to set-up this container
	 * for service execution Configuration resources and application components
	 * are made accessible using this method. <br />
	 * Resources can be exposed as files under a file-system or as resources
	 * accessible through java class loader. Typically, a file-system is
	 * convenient during development as the files can be changed without need to
	 * re-deploy/reboot the app.
	 *
	 *
	 * @param resourceRoot
	 *            if components are available on the file system, this is the
	 *            full path of the root folder where application.xml Otherwise
	 *            this is the prefix to application.xml. for example c:/a/a/c/
	 *            or /a/b/c such that c:/a/b/c/application.xml or
	 *            a.b.c.application.xml are available
	 *
	 * @return true if all OK. False in case of any set-up issue.
	 * @throws Exception
	 *             in case the root folder does not exist, or does not required
	 *             resources
	 */
	public static boolean bootStrap(String resourceRoot) throws Exception {
		String componentFolder = resourceRoot;

		if (componentFolder.endsWith(FOLDER_STR) == false) {
			componentFolder += FOLDER_STR;
		}
		logger.info("Bootstrapping with " + componentFolder);

		/*
		 * is this a folder
		 *
		 */
		Application app = new Application();
		String msg = null;
		try (InputStream ins = IoUtil.getStream(componentFolder + CONFIG_FILE_NAME)) {
			XmlUtil.xmlToObject(ins, app);
			if (app.applicationId == null) {
				msg = "Unable to load the configuration component " + CONFIG_FILE_NAME
						+ ". This file is expected to be inside folder " + componentFolder;
			} else {
				msg = app.configure(componentFolder);
			}
		} catch (Exception e) {
			msg = e.getMessage();
		}

		if (msg == null) {
			return true;
		}

		ApplicationError e = new ApplicationError(msg);
		ServiceRequest req = null;
		currentExceptionListener.listen(req, e);

		throw e;
	}

	/**
	 * unique name of this application within a corporate. This may be used as
	 * identity while trying to communicate with other applications within the
	 * corporate cluster
	 */
	@FieldMetaData(isRequired = true)
	String applicationId;

	/**
	 * list of modules in this application. We have made it mandatory to have a
	 * module, even if there is only one module. This is to enforce some
	 * discipline that retains flexibility for the app to be put into a context
	 * along with other apps.
	 */
	@FieldMetaData(isRequired = true)
	String[] modules;
	/**
	 * user id is a mandatory concept. Every service is meant to be executed for
	 * a specified (logged-in) user id. Apps can choose it to be either string
	 * or number
	 */
	boolean userIdIsNumber;

	/**
	 * do we cache components as they are loaded. typically true in production,
	 * and false in development environment
	 */
	boolean cacheComponents;

	/**
	 * during development/testing,we can simulate service executions with local
	 * data. service.xml is used for input/output, but the execution is skipped.
	 * json from data folder is used to populate serviceContext
	 */
	boolean simulateWithLocalData;

	/*
	 * app specific implementations of infrastructure/utility features
	 */
	/**
	 * Utility class that gets an instance of a Bean. Like the context in
	 * Spring. Useful when you want to work within a Spring container. Must
	 * implement <code>BeanFinderInterface</code> This is also used to get
	 * instance of any fully qualified name provided for configuration
	 */
	@FieldMetaData(superClass = BeanFinderInterface.class)
	String beanFinderClassName;
	/**
	 * Cache manager to be used by <code>ServiceAgent</code> to cache responses
	 * to services that are designed for caching. This class should implement
	 * <code>ServiceCacherInterface</code> null if caching is not to be enabled.
	 */
	@FieldMetaData(superClass = ServiceCacherInterface.class)
	String serviceCacherClassName;

	/**
	 * Service level access control to be implemented by
	 * <code>ServiceAgent</code> null if service agent is not responsible for
	 * this. Any service specific access control is to be managed by the service
	 * itself. must implement <code>AccessControllerInterface</code>
	 */
	@FieldMetaData(superClass = AccessControllerInterface.class)
	String accessControllerClassName;

	/**
	 * App specific hooks during service invocation life-cycle used by
	 * <code>ServiceAgent</code> null if no service agent is not responsible for
	 * this. Any service specific access control is to be managed by the service
	 * itself. must implement <code>ServicePrePostProcessorInterface</code>
	 */
	@FieldMetaData(superClass = ServicePrePostProcessorInterface.class)
	String servicePrePostProcessorClassName;

	/**
	 * way to wire exception to corporate utility. null if so such requirement.
	 * must implement <code>ExceptionListenerInterface</code>
	 */
	@FieldMetaData(superClass = ExceptionListenerInterface.class)
	String exceptionListenerClassName;

	/**
	 * class that can be used for caching app data. must implement
	 * <code>AppDataCacherInterface</code>
	 */
	@FieldMetaData(superClass = AppDataCacherInterface.class)
	String appDataCacherClassName;
	/**
	 * fully qualified class name that can be used for getting value for
	 * parameter/Property at run time. must implement
	 * <code>ParameterRetrieverInterface</code>
	 */
	@FieldMetaData(superClass = ParameterRetrieverInterface.class)
	String parameterRetrieverClassName;
	/**
	 * fully qualified class name that can be used for getting data/list source
	 * for dataAdapters. must implement <code>DataAdapterExtension</code>
	 */
	@FieldMetaData(superClass = DataAdapterExtension.class)
	String dataAdapterExtensionClassName;
	/**
	 * class name that implements <code>CommonCodeValidatorInterface</code>.
	 * null is no such concept used in this app
	 */
	@FieldMetaData(superClass = CommonCodeValidatorInterface.class)
	String commonCodeValidatorClassName;

	/**
	 * if attachments are managed by a custom code, specify the class name to
	 * wire it. It should implement <code>AttachmentAssistantInterface</code>
	 */
	@FieldMetaData(irrelevantBasedOnField = "attachmentsFolderPath", superClass = AttachmentAssistantInterface.class)
	String attachmentAssistantClassName;

	/**
	 * Simplity provides a rudimentary, folder-based system that can be used for
	 * storing and retrieving attachments. If you want to use that, provide the
	 * folder that is available for the server instance
	 */
	String attachmentsFolderPath;

	/** jndi name for user transaction for using JTA based transactions */
	String jtaUserTransaction;
	/**
	 * if JMS is used by this application, connection factory for local/session
	 * managed operations
	 */
	String jmsConnectionFactory;
	/** properties of jms connection, like user name password and other flags */
	Property[] jmsProperties;
	/**
	 * if JMS is used by this application, connection factory for JTA/JCA/XA
	 * managed operations
	 */
	String xaJmsConnectionFactory;

	/** batch job to fire after bootstrapping. */
	@FieldMetaData(isReferenceToComp = true, referredCompType = ComponentType.JOBS)
	String jobsToRunOnStartup;

	/** Configure the Mail Setup for the application */
	MailProperties mailProperties;
	/**
	 * OAuth parameters
	 */
	OAuthParameters oauthparameters;

	/*
	 * for batch jobs
	 */
	/** jndi name the container has created to get a threadFactory instance */
	String threadFactoryJndiName;

	/**
	 * jndi name the container has created to get a managed schedule thread
	 * pooled executor instance
	 */
	String scheduledExecutorJndiName;

	/** number of threads to keep in the pool even if they are idle */
	int corePoolSize;

	RdbDriver rdbDriver;

	private static OAuthParameters oauthparametersInternal;

	/**
	 *
	 * @return auth parameters
	 */
	public static OAuthParameters getOAuthParameters() {
		return oauthparametersInternal;
	}

	private static BeanFinderInterface classManagerInternal;

	/**
	 * gateways for external applications, indexed by id
	 */
	@FieldMetaData(packageName = "org.simplity.gateway", indexFieldName = "applicationName")
	Map<String, Gateway> externalApplications = new HashMap<>();

	/**
	 * configure application based on the settings. This MUST be triggered
	 * before using the app. Typically this would be triggered from start-up
	 * servlet in a web-app
	 *
	 * @param rootPath
	 *
	 * @return null if all OK. Else message that described why we could not
	 *         succeed.
	 */
	private String configure(String rootPath) {
		ComponentManager.bootstrap(rootPath, this.modules);
		List<String> msgs = new ArrayList<String>();
		if (this.beanFinderClassName != null) {
			try {
				classManagerInternal = (BeanFinderInterface) (Class.forName(this.beanFinderClassName)).newInstance();
			} catch (Exception e) {
				msgs.add(this.beanFinderClassName + " could not be used to instantiate a Class Manager. "
						+ e.getMessage());
			}
		} else {
			logger.info(
					"No app specific class manager/bean creater configured. Normal java class.forName() will be used to instantiate objects/beans");
		}

		int nbrErrors = 0;

		ExceptionListenerInterface listener = null;
		if (this.exceptionListenerClassName != null) {
			try {
				listener = Application.getBean(this.exceptionListenerClassName, ExceptionListenerInterface.class);
				currentExceptionListener = listener;
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No exception listener configured.");
		}

		ServiceCacherInterface casher = null;
		if (this.serviceCacherClassName != null) {
			try {
				casher = Application.getBean(this.serviceCacherClassName, ServiceCacherInterface.class);
				logger.info("{} is configured as class manager/bean finder.", this.serviceCacherClassName);
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No service cacher configured.");
		}

		AccessControllerInterface gard = null;
		if (this.accessControllerClassName != null) {
			try {
				gard = Application.getBean(this.accessControllerClassName, AccessControllerInterface.class);
				logger.info("{} is configured as access controller.", this.accessControllerClassName);
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No access controller configured.");
		}

		ServicePrePostProcessorInterface prePost = null;
		if (this.servicePrePostProcessorClassName != null) {
			try {
				prePost = Application.getBean(this.servicePrePostProcessorClassName,
						ServicePrePostProcessorInterface.class);
				logger.info("{} is configured as service pre-post-processor.", this.servicePrePostProcessorClassName);
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No service pre-post processor configured.");
		}

		/*
		 * rdb set up
		 */
		if (this.rdbDriver == null) {
			logger.info("No rdb has been set up for this app.");
		} else {
			String msg = this.rdbDriver.setup();
			if (msg != null) {
				msgs.add(msg);
			}
		}

		if (this.appDataCacherClassName != null) {
			try {
				currentAppDataCacher = Application.getBean(this.appDataCacherClassName, AppDataCacherInterface.class);
				logger.info("{} is used as app data cacher", this.appDataCacherClassName);
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No app data cacher configured.");
		}

		if (this.commonCodeValidatorClassName != null) {
			try {
				CommonCodeValidatorInterface val = Application.getBean(this.commonCodeValidatorClassName,
						CommonCodeValidatorInterface.class);
				if (val != null) {
					CommonCodeValidator.setValidator(val);
					logger.info("{} is used as common code validator", this.commonCodeValidatorClassName);
				}
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No common code validator configured.");
		}

		if (this.parameterRetrieverClassName != null) {
			try {
				ParameterRetrieverInterface val = Application.getBean(this.parameterRetrieverClassName,
						ParameterRetrieverInterface.class);
				if (val != null) {
					ParameterRetriever.setRetriever(val);
					logger.info("{} is used as parameter retriever", this.parameterRetrieverClassName);
				}
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No parameter retriever configured.");
		}

		if (this.dataAdapterExtensionClassName != null) {
			try {
				DataAdapterExtension val = Application.getBean(this.dataAdapterExtensionClassName,
						DataAdapterExtension.class);
				if (val != null) {
					ServiceContext.setDataAdapterExtension(val);
					logger.info("{} is used as data adapter extension ", this.dataAdapterExtensionClassName);
				}
			} catch (Exception e) {
				nbrErrors++;
			}
		} else {
			logger.info("No data adapter extension configured.");
		}

		if (this.jtaUserTransaction != null) {
			try {
				userTransactionInstance = new InitialContext().lookup(this.jtaUserTransaction);
				if (userTransactionInstance instanceof UserTransaction == false) {
					msgs.add(this.jtaUserTransaction + " is located but it is not UserTransaction but "
							+ userTransactionInstance.getClass().getName());
				} else {

					logger.info("userTransactionInstance set to " + userTransactionInstance.getClass().getName());
				}
			} catch (Exception e) {
				msgs.add("Error while instantiating UserTransaction using jndi name " + this.jtaUserTransaction + ". "
						+ e.getMessage());
			}
		}
		/*
		 * Setup JMS Connection factory
		 */
		if (this.jmsConnectionFactory != null || this.xaJmsConnectionFactory != null) {
			String msg = JmsConnector.setup(this.jmsConnectionFactory, this.xaJmsConnectionFactory, this.jmsProperties);
			if (msg != null) {
				msgs.add(msg);
			}
		}

		/*
		 * Setup Mail Agent
		 */
		if (this.mailProperties != null) {
			try {
				MailConnector.initialize(this.mailProperties);
			} catch (Exception e) {
				msgs.add("Error while setting up MailAgent." + e.getMessage() + " Application will not work properly.");
			}
		}

		/*
		 * in production, we cache components as they are loaded, but in
		 * development we prefer to load the latest
		 */
		if (this.cacheComponents) {
			ComponentManager.startCaching();
		}

		/*
		 * what about file/media/attachment storage assistant?
		 */
		AttachmentAssistantInterface ast = null;
		if (this.attachmentsFolderPath != null) {
			ast = new FileBasedAssistant(this.attachmentsFolderPath);
		} else if (this.attachmentAssistantClassName != null) {
			try {
				ast = Application.getBean(this.attachmentAssistantClassName, AttachmentAssistantInterface.class);
			} catch (Exception e) {
				nbrErrors++;
			}
		}
		if (ast != null) {
			AttachmentManager.setAssistant(ast);
		}

		userIdIsNumeric = this.userIdIsNumber;

		/*
		 * initialize service agent
		 */
		ServiceAgent.setUp(this.userIdIsNumber, casher, gard, prePost, this.simulateWithLocalData);

		/*
		 * batch job, thread pools etc..
		 */
		if (this.corePoolSize == 0) {
			batchPoolSize = 1;
		} else {
			batchPoolSize = this.corePoolSize;
		}

		if (this.threadFactoryJndiName != null) {
			try {
				threadFactory = (ThreadFactory) new InitialContext().lookup(this.threadFactoryJndiName);

				logger.info("Thread factory instantiated as " + threadFactory.getClass().getName());

			} catch (Exception e) {
				msgs.add("Error while looking up " + this.threadFactoryJndiName + ". " + e.getLocalizedMessage());
			}
		}

		if (this.scheduledExecutorJndiName != null) {
			try {
				threadPoolExecutor = (ScheduledExecutorService) new InitialContext()
						.lookup(this.scheduledExecutorJndiName);

				logger.info("ScheduledThreadPoolExecutor instantiated as " + threadPoolExecutor.getClass().getName());

			} catch (Exception e) {
				msgs.add("Error while looking up " + this.scheduledExecutorJndiName + ". " + e.getLocalizedMessage());
			}
		}

		String result = null;

		/*
		 * gate ways
		 */

		if (this.externalApplications.isEmpty() == false) {
			Gateways.setGateways(this.externalApplications);
			this.externalApplications = null;
		}

		if (msgs.size() > 0) {
			/*
			 * we got errors.
			 */
			StringBuilder err = new StringBuilder("Error while bootstrapping\n");
			for (String msg : msgs) {
				err.append(msg).append('\n');
			}
			/*
			 * we run the background batch job only if everything has gone well.
			 */
			if (this.jobsToRunOnStartup != null) {
				err.append("Scheduler NOT started for batch " + this.jobsToRunOnStartup
						+ " because of issues with applicaiton set up.");
				err.append('\n');
			}
			result = err.toString();

			logger.info(result);

		} else if (nbrErrors > 0) {
			result = " one or more error while using class names for object instantiation. Refer to erro rlogs";
		} else if (this.jobsToRunOnStartup != null) {
			/*
			 * we run the background batch job only if everything has gone well.
			 */
			BatchJobs.startJobs(this.jobsToRunOnStartup);

			logger.info("Scheduler started for Batch " + this.jobsToRunOnStartup);
		}
		if (this.oauthparameters != null) {
			oauthparametersInternal = this.oauthparameters;
		}
		return result;
	}

	/**
	 * validate all field values of this as a component
	 *
	 * @param vtx
	 *            validation context
	 */
	public void validate(ValidationContext vtx) {
		ValidationUtil.validateMeta(vtx, this);
		if (this.rdbDriver != null) {
			this.rdbDriver.validate(vtx);
		}

		if (this.attachmentsFolderPath != null) {
			File file = new File(this.attachmentsFolderPath);
			if (file.exists() == false) {
				vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
						this.attachmentsFolderPath + " is not a valid path in the system file system.",
						"attachmentsFolderPath"));
			}
		}
	}

	/**
	 * get a bean from the container
	 *
	 * @param className
	 * @param clazz
	 * @return instance of the class, or null if such an object could not be
	 *         located
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getBean(String className, Class<T> clazz) {
		T tb = null;
		if (classManagerInternal != null) {
			tb = classManagerInternal.getBean(className, clazz);
		}
		if (tb != null) {
			return tb;
		}
		try {
			tb = (T) (Class.forName(className)).newInstance();
		} catch (Exception e) {
			throw new ApplicationError(className + " is not a valid class that implements " + clazz.getName());
		}
		return tb;
	}

	/**
	 * @param userId
	 * @return app user for this user id
	 */
	public static AppUser ceateAppUser(String userId) {
		Value uid = null;
		if (userIdIsNumeric) {
			uid = Value.parseValue(userId, ValueType.INTEGER);
		} else {
			uid = Value.newTextValue(userId);
		}
		return new AppUser(uid);
	}
}
