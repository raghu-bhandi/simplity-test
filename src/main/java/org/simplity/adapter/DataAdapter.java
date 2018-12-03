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
import org.simplity.kernel.comp.Component;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.service.ServiceContext;

/**
 * Specifications for copying data from source to target
 *
 * @author simplity.org
 *
 */

public class DataAdapter implements Component {

	/**
	 * unique name within a module
	 */
	String name;

	/**
	 * optional. adapters can be organized nicely into modules. Also, this helps
	 * in creating sub-apps
	 */
	String moduleName;

	/**
	 * fields to be copied using this adapter
	 */
	AbstractField[] fields;

	/**
	 * input data from source to VO
	 *
	 * @param source
	 * @param target
	 * @param ctx
	 */
	public void copy(DataSource source, DataTarget target, ServiceContext ctx) {
		for (AbstractField field : this.fields) {
			field.copy(source, target, ctx);
		}
	}

	/**
	 *
	 * @return fields
	 */
	public AbstractField[] getFields() {
		return this.fields;
	}

	@Override
	public void getReady() {
		if (this.fields == null) {
			throw new ApplicationError("Adapter requires one or more input fields");
		}
		for (AbstractField field : this.fields) {
			field.getReady();
		}
	}

	@Override
	public String getSimpleName() {
		return this.name;
	}

	@Override
	public String getQualifiedName() {
		if (this.moduleName == null) {
			return this.name;
		}
		return this.moduleName + '.' + this.name;
	}

	@Override
	public void validate(ValidationContext ctx) {
		// return 0;
	}

	@Override
	public ComponentType getComponentType() {
		return ComponentType.ADAPTER;
	}
}
