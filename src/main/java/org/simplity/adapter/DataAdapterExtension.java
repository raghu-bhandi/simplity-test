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

package org.simplity.adapter;

import org.simplity.adapter.source.DataListSource;
import org.simplity.adapter.source.DataSource;
import org.simplity.adapter.target.DataListTarget;
import org.simplity.adapter.target.DataTarget;
import org.simplity.service.ServiceContext;

/**
 * Means to create app-specific data source and data targets. App specific
 * implementation should register this using Applicaiton.xml
 *
 * @author simplity.org
 *
 */
public interface DataAdapterExtension {

	/**
	 * called by <code>ContextDataSource</code> when it finds a non-json object
	 * as a candidate data source
	 *
	 * @param obj
	 *            object found in the context with the field name. CAN BE NULL.
	 *            this is to ensure that app-specific hook has the opportunity
	 *            to have even name-specific algorithm. IMplementations MUST
	 *            check for null before proceeding
	 * @param fieldName
	 * @param ctx
	 * @return data source wrapper for the object found in the ctx with that
	 *         field name for an adapter to copy data into it. null if this is
	 *         not possible, so that caller can look for other opportunities
	 */
	DataSource getDataSource(Object obj, String fieldName, ServiceContext ctx);

	/**
	 * called by <code>ContextDataSource</code> when it finds a non-json object
	 * as a candidate data list source
	 *
	 * @param obj
	 *            object found in the context with the field name. CAN BE NULL.
	 *            this is to ensure that app-specific hook has the opportunity
	 *            to have even name-specific algorithm. IMplementations MUST
	 *            check for null before proceeding
	 * @param fieldName
	 * @param ctx
	 * @return list data source wrapper for the object found in the ctx with
	 *         that field name for an adapter to copy list of data into it
	 */
	DataListSource getListSource(Object obj, String fieldName, ServiceContext ctx);

	/**
	 * called by <code>ContextDataSource</code> when it finds a non-json object
	 * as a value for its method getObjectValue(). Implementation may use this
	 * hook to treat object/list compatibility.
	 *
	 * @param obj
	 *            object found in the context with the field name. non-null.
	 * @param fieldName
	 * @param ctx
	 * @return by default it should be obj. could be the first member of obj, if
	 *         it happens to be a list
	 */
	Object getObjectValue(Object obj, String fieldName, ServiceContext ctx);

	/**
	 * called by <code>ContextDataTarget</code> when it is trying to create a
	 * data target.
	 *
	 * @param obj
	 *            object found in the context with the field name. CAN BE NULL.
	 *            this is to ensure that app-specific hook has the opportunity
	 *            to have even name-specific algorithm. IMplementations MUST
	 *            check for null before proceeding
	 * @param targetClassName
	 *            if this info is available at run time. can be null.
	 * @param fieldName
	 * @param ctx
	 * @return data target wrapper for the object and/or className. null if this
	 *         is not possible, so that caller can look for other opportunities
	 */
	DataTarget getDataTarget(Object obj, String targetClassName, String fieldName, ServiceContext ctx);

	/**
	 * called by <code>ContextDataTarget</code> when it is trying to create a
	 * list target.
	 *
	 * @param obj
	 *            object found in the context with the field name. CAN BE NULL.
	 *            this is to ensure that app-specific hook has the opportunity
	 *            to have even name-specific algorithm. IMplementations MUST
	 *            check for null before proceeding
	 * @param targetClassName
	 *            if this info is available at run time. can be null.
	 * @param fieldName
	 * @param ctx
	 * @return list target wrapper for the object and/or className. null if this
	 *         is not possible, so that caller can look for other opportunities
	 */
	DataListTarget getListTarget(Object obj, String targetClassName, String fieldName, ServiceContext ctx);

}
