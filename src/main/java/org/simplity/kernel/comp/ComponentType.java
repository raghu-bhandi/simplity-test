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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.simplity.adapter.DataAdapter;
import org.simplity.job.BatchJobs;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.Message;
import org.simplity.kernel.db.Sql;
import org.simplity.kernel.db.StoredProcedure;
import org.simplity.kernel.dm.Record;
import org.simplity.kernel.dt.DataType;
import org.simplity.kernel.fn.Concat;
import org.simplity.kernel.fn.Function;
import org.simplity.kernel.util.XmlUtil;
import org.simplity.service.ExternalService;
import org.simplity.test.TestRun;
import org.simplity.tp.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * components are the basic building blocks of application. This is an
 * enumeration of them. Unlike typical Enum class, this one is quite
 * comprehensive with utility methods to load/cache components
 *
 * @author simplity.org
 */
public enum ComponentType {
	/** Data Type */
	DT(0, DataType.class, "dt/", true),
	/** Message */
	MSG(1, Message.class, "msg/", true),
	/** Record */
	REC(2, Record.class, "rec/", false),
	/** service */
	SERVICE(3, Service.class, "service/", false) {
		/*
		 * (non-Javadoc)
		 *
		 * @see
		 * org.simplity.kernel.comp.ComponentType#getComponentOrNull(java.lang.
		 * String)
		 */
		@Override
		public Component getComponentOrNull(String compName) {
			Component comp = super.getComponentOrNull(compName);
			if (comp != null) {
				return comp;
			}
			comp = Service.generateService(compName);
			if (comp == null) {
				return null;
			}
			logger.info("Service compName is generated on-the-fly and is used as a regular service");
			comp.getReady();
			if (this.cachedOnes != null) {
				this.cachedOnes.put(compName, comp);
			}
			return comp;
		}
	},
	/** Sql */
	SQL(4, Sql.class, "sql/", false),
	/** Stored procedure */
	SP(5, StoredProcedure.class, "sp/", false),
	/** function */
	FUNCTION(6, Function.class, "fn/", true),

	/** test cases for service */
	TEST_RUN(7, TestRun.class, "test/", false),

	/** test cases for service */
	JOBS(8, BatchJobs.class, "jobs/", false),
	/**
	 * external services
	 */
	EXTERN(9, ExternalService.class, "extern/", false),
	/**
	 * data adapter
	 */
	ADAPTER(10, DataAdapter.class, "adapter/", false);

	protected static final Logger logger = LoggerFactory.getLogger(ComponentType.class);

	/*
	 * constants
	 */
	private static final char FOLDER_CHAR = '/';
	private static final char DELIMITER = '.';
	private static final String EXTN = ".xml";
	private static final String BUILT_IN_NAME = "_system";
	private static final String BUILT_IN_COMP_PREFIX = "org/simplity/comp/";

	protected static String[] modules;

	/*
	 * list of built-in functions
	 */
	protected static final Function[] BUILT_IN_FUNCTIONS = { new Concat() };

	/**
	 * path where we can find applicaiton.xml
	 */
	private static String compRootPath = "comp/";

	/**
	 * service has a way to generate rather than load.. One way is to have a
	 * class associated with that
	 */
	protected static final Map<String, Object> serviceAliases = new HashMap<String, Object>();
	/*
	 * attributes of component type
	 */
	/**
	 * allows us to use array instead of map while dealing with componentType
	 * based collections
	 */
	private final int idx;

	/** class associated with this type that is used for loading component/s */
	protected final Class<?> cls;

	/** folder name under which components are saved */
	protected final String folder;

	/** is this loaded on a need basis or pre-loaded? */
	private final boolean isPreloaded;

	protected Map<String, Object> cachedOnes;

	/**
	 * @param idx
	 * @param cls
	 * @param folder
	 */
	ComponentType(int idx, Class<? extends Component> cls, String folder, boolean preLoaded) {
		this.idx = idx;
		this.cls = cls;
		this.folder = folder;
		this.isPreloaded = preLoaded;
		if (this.isPreloaded) {
			this.cachedOnes = new HashMap<String, Object>();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString() {
		return this.cls.getSimpleName();
	}

	/**
	 * @return idx associated with this comp type
	 */
	public int getIdx() {
		return this.idx;
	}

	/**
	 * @return folder name within the component folder. e.g. service/tp/ for
	 *         service
	 */
	public String getFolderPrefix() {
		return this.folder;
	}

	/**
	 * @return true if it is pre-loaded. false if it is loaded on a need basis
	 */
	public boolean isPreloaded() {
		return this.isPreloaded;
	}

	/**
	 *
	 * @return the base class of the underlying component
	 */
	public Class<?> getComponentClass() {
		return this.cls;
	}

	/**
	 * @param compName
	 *            qualified component name
	 * @return instance of the desired component. Throws ApplicationError if
	 *         this component is not found. use getComponentOrNull() if you do
	 *         not want an error
	 */
	public Component getComponent(String compName) {
		Component comp = this.getComponentOrNull(compName);
		if (comp == null) {
			throw new MissingComponentError(this, compName);
		}
		return comp;
	}

	/**
	 * @param compName
	 *            qualified component name
	 * @return instance of the desired component. Throws ApplicationError if
	 *         this component is not found. use getComponentOrNull() if you do
	 *         not want an error
	 */
	public Component getComponentOrNull(String compName) {
		/*
		 * do we have it in our cache?
		 */
		if (this.cachedOnes != null) {
			Object object = this.cachedOnes.get(compName);
			if (object != null) {
				return (Component) object;
			}
		}

		/*
		 * look no further if this is always cached
		 */
		if (this.isPreloaded) {
			return null;
		}

		Component comp = this.load(compName);
		if (comp == null) {
			return null;
		}

		if (this.cachedOnes != null) {
			this.cachedOnes.put(compName, comp);
		}
		return comp;
	}

	/**
	 * get all pre-loaded Components
	 *
	 * @return map of all pre-loaded components
	 * @throws ApplicationError
	 *             in case this type is not pre-loaded
	 */
	public Collection<Object> getAll() {
		if (this.isPreloaded) {
			return this.cachedOnes.values();
		}
		throw new ApplicationError(this + " is not pre-loaded and hence we can not respond to getAll()");
	}

	/**
	 * replace the component in the cache.
	 *
	 * @param comp
	 */
	public void replaceComponent(Component comp) {
		if (this.cachedOnes == null || comp == null) {
			return;
		}

		if (this.cls.isInstance(comp)) {
			String name = comp.getQualifiedName();
			this.cachedOnes.put(name, comp);
			logger.info("{} replaced", name);
		} else {
			throw new ApplicationError(
					"An object of type " + comp.getClass().getName() + " is being passed as component " + this);
		}
	}

	/**
	 * remove the component from cache.
	 *
	 * @param compName
	 *            fully qualified name
	 */
	public void removeComponent(String compName) {
		if (this.cachedOnes != null) {
			this.cachedOnes.remove(compName);
		}
	}

	/**
	 * load a component from storage into an instance
	 *
	 * @param compName
	 * @return initialized component, or null if it is not found
	 */
	private Component load(String compName) {
		String fileName = compRootPath + this.folder + compName.replace(DELIMITER, FOLDER_CHAR) + EXTN;
		Exception exp = null;
		Object obj = null;
		try {
			obj = this.cls.newInstance();
			if (XmlUtil.xmlToObject(fileName, obj) == false) {
				/*
				 * load failed. obj is not valid any more.
				 */
				obj = null;
			}
		} catch (Exception e) {
			exp = e;
		}

		if (exp != null) {
			logger.error("error while loading component " + compName, exp);
			return null;
		}
		if (obj == null) {
			logger.info("Component {} is not loaded. Either it is not defined, or it has syntax errors.", compName);
			return null;
		}
		/*
		 * we insist that components be stored with the right naming convention
		 */
		Component comp = (Component) obj;
		String fullName = comp.getQualifiedName();

		if (compName.equals(fullName) == false) {
			logger.info("Component has a qualified name of {}  that is different from its storage name {}", fullName,
					compName);
			return null;
		}
		comp.getReady();
		return comp;
	}

	/**
	 * load all components inside folder. This is used by components that are
	 * pre-loaded. These are saved as collections, and not within their own
	 * files
	 *
	 * @param folder
	 * @param packageName
	 * @param objects
	 */
	private void loadAll() {
		try {
			String packageName = this.cls.getPackage().getName() + '.';
			/*
			 * load system-defined on
			 */
			this.loadOne(BUILT_IN_COMP_PREFIX + this.folder + BUILT_IN_NAME + EXTN, packageName);

			/*
			 * load one for each module
			 */
			String prefix = compRootPath + this.folder;
			for (String module : modules) {
				this.loadOne(prefix + module + EXTN, packageName);
			}
			/*
			 * we have to initialize the components
			 */
			for (Object obj : this.cachedOnes.values()) {
				((Component) obj).getReady();
			}
			logger.info("{} {} loaded.", this.cachedOnes.size(), this);
		} catch (Exception e) {
			this.cachedOnes.clear();
			logger.error(
					"pre-loading of " + this
							+ " failed. No component of this type is available till we successfully pre-load them again.",
					e);
		}
	}

	private void loadOne(String resName, String packageName) {
		logger.info("Going to load components from {}", resName);
		try {
			XmlUtil.xmlToCollection(resName, this.cachedOnes, packageName);
		} catch (Exception e) {
			logger.error("Resource " + resName + " failed to load.", e);
		}
	}

	/*
	 * static methods that are used by infra-set up to load/cache components
	 */
	/**
	 * let components be cached once they are loaded. Typically used in
	 * production environment
	 */
	static void startCaching() {
		/*
		 * component caching happens if the collection exists
		 */
		for (ComponentType aType : ComponentType.values()) {
			if (aType.isPreloaded == false) {
				aType.cachedOnes = new HashMap<String, Object>();
			}
		}
	}

	/**
	 * purge cached components, and do not cache any more. USed during
	 * development.
	 */
	static void stopCaching() {
		/*
		 * remove existing cache for loaded-on-demand components. Also, null
		 * implies that they are not be cached
		 */
		for (ComponentType aType : ComponentType.values()) {
			if (aType.isPreloaded == false) {
				aType.cachedOnes = null;
			}
		}
	}

	/**
	 * set the root path for components and reset/reload all components
	 *
	 * @param rootPath
	 *            root path. Valid URL path. We encourage resource syntax rather
	 *            than file syntax
	 */
	static void bootstrap(String rootPath, String[] moduleNames) {
		compRootPath = rootPath;
		modules = moduleNames;
		logger.info("Root path for components set to {}", compRootPath);

		for (ComponentType aType : ComponentType.values()) {
			if (aType.isPreloaded) {
				aType.loadAll();
			} else if (aType.cachedOnes != null) {
				aType.cachedOnes.clear();
			}
		}
	}

	/**
	 * @return return the root path for components. This is the root path under
	 *         which all components can be located.
	 */
	public static String getComponentRoot() {
		return compRootPath;
	}

	/**
	 * find out what type of resource is expected based on the folder-prefix
	 *
	 * @param fileName
	 *            file name relative to to component root. for example
	 *            dt/internal.xml or service/mod/s.xml
	 * @return component type based on the folder, or null if no component type
	 *         expects the folder structure
	 */
	public static ComponentType getTypeByFolder(String fileName) {
		for (ComponentType ct : ComponentType.values()) {
			if (fileName.startsWith(ct.getFolderPrefix())) {
				return ct;
			}
		}
		return null;
	}
}
