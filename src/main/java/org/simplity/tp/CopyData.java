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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.simplity.tp;

import org.simplity.adapter.DataAdapter;
import org.simplity.adapter.source.ContextDataSource;
import org.simplity.adapter.source.DataSource;
import org.simplity.adapter.target.ContextDataTarget;
import org.simplity.adapter.target.DataTarget;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * copy data across data objects, like JSON, POJO etc.. based on a DataAdapter
 * specification
 *
 * @author simplity.org
 */
public class CopyData extends Action {
	private static final Logger logger = LoggerFactory.getLogger(CopyData.class);

	/**
	 * fully qualified name of adapter to be used for copying data
	 */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.ADAPTER)
	String adapterName;

	/**
	 * name of the object/data sheet to copy data from. skip this if data from
	 * service context is to be copied
	 */
	String sourceObjectName;

	/**
	 * name of target object. skip this if data is to be copied to the service
	 * context itself
	 */

	String targetObjectName;

	/**
	 * fully qualified name of the class to be used to create the target object.
	 * Skip this if the object is already available in the context
	 */
	String targetClassName;

	@Override
	protected Value doAct(ServiceContext ctx) {
		DataAdapter adapter = ComponentManager.getAdapter(this.adapterName);
		DataSource source = null;
		DataTarget target = null;
		if (this.sourceObjectName != null) {
			source = ctx.getDataSource(this.sourceObjectName);
		} else {
			source = new ContextDataSource(ctx);
		}
		if (this.targetObjectName != null) {
			target = ctx.getDataTarget(this.targetObjectName, this.targetClassName);
		} else {
			target = new ContextDataTarget(ctx);
		}
		adapter.copy(source, target, ctx);
		return Value.VALUE_TRUE;
	}

	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		if (this.sourceObjectName == null && this.targetObjectName == null) {
			logger.warn("Going to copy data from context back into context. Are you sure that is what you want?");
		}
	}
}
