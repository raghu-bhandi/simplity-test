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

package org.simplity.adapter;

import org.simplity.adapter.source.DataSource;
import org.simplity.adapter.target.DataTarget;
import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * field that is non-primitive non-list object. Note that either the source, or
 * the target may not have a sub-object, but continue to receive/copy fields
 * from/to same target/source
 *
 * This field may also be used to include a group of fields that are already
 * defined in another adapter. THis feature improves modularity and DRY
 *
 * @author simplity.org
 *
 */
public class ObjectField extends AbstractField {
	private static final Logger logger = LoggerFactory.getLogger(ObjectField.class);
	/**
	 * class name of the object. optional. generally used if non-json
	 * app-specific objects are to be managed
	 */
	String targetClassName;
	/**
	 * fields to be copied
	 */
	AbstractField[] fields;

	/**
	 * adapter from which to copy/include fields into this specification
	 */
	String referredAdapter;

	@Override
	public void copy(DataSource source, DataTarget target, ServiceContext ctx) {
		if (this.fields == null) {
			/*
			 * copy object directly from source to target, not field-by-field
			 */
			Object obj = source.getObjectValue(this.fromName);
			if (obj == null) {
				logger.info("No object value for {} in source. data not copied", this.fromName);
				return;
			}
			target.setObjectValue(this.toName, obj);
			return;
		}
		DataSource childSource = source;
		/*
		 * fromName null implies that the data for the target object comes from
		 * the current object, and not a child object
		 */
		if (this.fromName != null) {
			childSource = source.getChildSource(this.fromName);
			if (childSource == null) {
				logger.info("No child source named {}. data not copied", this.fromName);
				return;
			}
		}

		DataTarget childTarget = target;
		if (this.toName != null) {
			childTarget = target.getChildTarget(this.toName, this.targetClassName);
			if (childTarget == null) {
				logger.info("No child target named {}. data not copied", this.toName);
				return;
			}
		}
		for (AbstractField f : this.fields) {
			f.copy(childSource, childTarget, ctx);
		}
	}

	@Override
	public void getReady() {
		super.getReady();
		if (this.fromName == null && this.toName == null && this.fields == null) {
			logger.warn(
					"This field is serving no purpose. Fields from this can as well be included in to the parent list");
		}

		if (this.referredAdapter != null) {
			DataAdapter ref = ComponentManager.getAdapter(this.referredAdapter);
			this.fields = ref.getFields();
		} else {
			if (this.fields == null) {
				if (this.fromName == null || this.toName == null) {
					throw new ApplicationError(
							"When fields aare not specified, object field should have both fromName and toNAme");
				}
				logger.info(
						"Object field {} is used to copy an entire object from source to target. Object level compatibility is required for this.");
			} else {
				for (AbstractField field : this.fields) {
					field.getReady();
				}
			}
		}
	}
}
